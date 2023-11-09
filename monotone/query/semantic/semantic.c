
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>

static inline bool
semantic_compare(Target* target, Ast* ast, Key* key)
{
	auto column = def_column_of(table_def(target->table), key->column);

	// column
	if (ast->id == KNAME)
	{
		// match by column and key is not nested
		if (! str_compare(&ast->string, &column->name))
			return false;
		return str_empty(&key->path);
	}

	// [target.]column[.path]
	if (ast->id != KNAME_COMPOUND)
		return false;

	Str path;
	str_set_str(&path, &ast->string);
	Str name;
	str_split_or_set(&ast->string, &name, '.');

	// [target.]
	if (target_compare(target, &name))
	{
		str_advance(&path, str_size(&name) + 1);

		// skip target name
		str_split_or_set(&path, &name, '.');
	}

	// column[.path]
	bool compound = str_split_or_set(&path, &name, '.');
	if (! str_compare(&name, &column->name))
		return false;

	// column
	if (! compound)
		return str_empty(&key->path);

	// skip column name
	str_advance(&path, str_size(&name) + 1);

	// path
	if (! str_empty(&key->path))
		return str_compare(&path, &key->path);

	return false;
}

static inline Ast*
semantic_key(TargetList* target_list, Target* target, Ast* op, Key* key)
{
	auto l = op->l;
	auto r = op->r;

	//
	// name = expr
	// expr = name
	//
	Ast* expr_key = NULL;
	Ast* expr = NULL;
	if (semantic_compare(target, l, key))
	{
		expr_key = l;
		expr     = r;
	} else
	if (semantic_compare(target, r, key))
	{
		expr_key = r;
		expr     = l;
	}
	if (expr_key == NULL)
		return NULL;

	// validate expr type
	if (expr->id == KINT)
	{
		if (unlikely(key->type != TYPE_INT))
			return NULL;
	} else
	if (expr->id == KSTRING)
	{
		if (unlikely(key->type != TYPE_STRING))
			return NULL;
	}

	// join: name = name

	// do not match keys for outer targets
	if (expr->id == KNAME_COMPOUND)
	{
		Str name;
		str_split_or_set(&expr->string, &name, '.');

		auto join = target_list_match(target_list, &name);
		if (join)
		{
			if (join == target)
				return NULL;

			// FROM target, join
			if (target->level == join->level)
				if (target->level_seq < join->level_seq)
					return NULL;

			// SELECT (SELECT FROM join) FROM target
			if (target->level < join->level)
				return NULL;

			// SELECT (SELECT FROM target) FROM join
		}
	}

	return expr;
}

hot static inline int
semantic_compare_ast(Ast* a, Ast* b)
{
	if (a->id == KINT)
	{
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	}
	if (a->id == KSTRING)
		return str_compare_fn(&a->string, &b->string);
	assert(0);
	return 0;
}

hot void
semantic(TargetList* target_list, Target* target, AstList* ops)
{
	if (target->keys || !target->table)
		return;

	auto def = table_def(target->table);

	// prepare target keys
	target->keys = target_key_create(def);

	int key_matched_stop = 0;
	int key_matched_equ  = 0;
	int key_matched      = 0;

	for (auto key = def->key; key; key = key->next)
	{
		auto node = ops->list;
		for (; node; node = node->next)
		{
			auto op   = node->ast;
			auto expr = semantic_key(target_list, target, op, key);
			if (expr == NULL)
				continue;

			// key found with expected type
			auto ref  = &target->keys[key->order];
			switch (op->id) {
			case '>':
			case KGTE:
			case '=':
			{
				// start condition
				if (ref->start == NULL)
				{
					ref->start_op = op;
					ref->start    = expr;
					key_matched++;
					if (op->id == '=')
						key_matched_equ++;
					break;
				}

				// do not redefine first point lookup
				if (ref->start_op->id == '=')
					break;

				// force as point lookup if key is already defined
				if (op->id == '=')
				{
					ref->start_op = op;
					ref->start    = expr;
					key_matched_equ++;
					break;
				}

				// update key if it is > then the previous one
				if (semantic_compare_ast(ref->start, expr) == -1)
				{
					ref->start_op = op;
					ref->start    = expr;
				}
				break;
			}
			case '<':
			case KLTE:
			{
				// stop condition
				if (ref->stop == NULL)
				{
					ref->stop_op = op;
					ref->stop    = expr;
					key_matched_stop++;
					break;
				}

				// update key if it is < then the previous one
				if (semantic_compare_ast(ref->stop, expr) > 0)
				{
					ref->stop_op = op;
					ref->stop    = expr;
				}
				break;
			}
			default:
				continue;
			}
		}
	}

	// point lookup
	if (key_matched_equ == table_def(target->table)->key_count)
		target->is_point_lookup = true;

	// has scan stop expressions <, <=
	if (key_matched_stop > 0)
		target->stop_ops = true;
}
