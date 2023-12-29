
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>

hot static inline int
plan_compare_ast(Ast* a, Ast* b)
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

static inline bool
plan_compare(Target* target, Ast* ast, Key* key)
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

static inline bool
plan_key_is(TargetList* target_list, Target* target, Key* key,
            Ast* path, Ast* value)
{
	// compare path with column key
	if (! plan_compare(target, path, key))
		return false;

	// validate value to key type
	if (value->id == KINT)
	{
		if (unlikely(key->type != TYPE_INT))
			return false;
	} else
	if (value->id == KSTRING)
	{
		if (unlikely(key->type != TYPE_STRING))
			return false;
	}

	// join: name = name

	// do not match keys for outer targets
	if (value->id == KNAME_COMPOUND)
	{
		Str name;
		str_split_or_set(&path->string, &name, '.');

		auto join = target_list_match(target_list, &name);
		if (join)
		{
			if (join == target)
				return false;

			// FROM target, join
			if (target->level == join->level)
				if (target->level_seq < join->level_seq)
					return false;

			// SELECT (SELECT FROM join) FROM target
			if (target->level < join->level)
				return false;

			// SELECT (SELECT FROM target) FROM join
		}
	}

	return true;
}

hot static inline Key*
plan_key_find(TargetList* target_list, Target* target, Ast* path, Ast* value)
{
	auto def = table_def(target->table);
	for (auto key = def->key; key; key = key->next)
		if (plan_key_is(target_list, target, key, path, value))
			return key;
	return NULL;
}

hot static inline AstPlan*
plan_op(TargetList* target_list, Target* target,
        Ast* op,
        Ast* path,
        Ast* value)
{
	// find matching key
	auto key = plan_key_find(target_list, target, path, value);
	if (! key)
		return NULL;

	auto plan     = ast_plan_allocate(table_def(target->table), SCAN);
	auto plan_key = &plan->keys[key->order];
	switch (op->id) {
		break;
	case '>':
	case KGTE:
	case '=':
	{
		plan_key->start_op = op;
		plan_key->start    = value;
		if (op->id == '=')
		{
			auto def = table_def(target->table);
			if (def->key_count == 1)
				plan->type = SCAN_LOOKUP;
		}
		break;
	}
	case '<':
	case KLTE:
	{
		// stop condition works only on the outer key
		if (key->order > 0)
			break;
		plan_key->stop_op = op;
		plan_key->stop    = value;
		break;
	}
	}

	return plan;
}

hot static inline bool
plan_op_of(Ast* expr, Ast** path, Ast** value)
{
	//
	// name = expr
	// expr = name
	//
	if (expr->l->id == KNAME || expr->l->id == KNAME_COMPOUND)
	{
		if (expr->r->id == KINT || expr->r->id == KSTRING)
		{
			*path  = expr->l;
			*value = expr->r;
			return true;
		}
	} else
	if (expr->r->id == KNAME || expr->r->id == KNAME_COMPOUND)
	{
		if (expr->l->id == KINT || expr->l->id == KSTRING)
		{
			*path  = expr->r;
			*value = expr->l;
			return true;
		}
	}
	return false;
}

hot static inline bool
plan_and_merge_start(Key* key, AstPlan* l, AstPlan *r)
{
	auto ref_l = &l->keys[key->order];
	auto ref_r = &r->keys[key->order];

	if (ref_l->start && ref_r->start)
	{
		if (ref_l->start_op->id == '=')
		{
			// do not redefine first point lookup
		} else
		{
			// force as point lookup if
			if (ref_r->start_op->id == '=')
			{
				ref_l->start_op = ref_r->start_op;
				ref_l->start    = ref_r->start;
			} else
			{
				// update key if it is > then the previous one
				if (plan_compare_ast(ref_l->start, ref_r->start) == -1)
				{
					ref_l->start_op = ref_r->start_op;
					ref_l->start    = ref_r->start;
				}
			}
		}
	} else
	if (ref_r->start)
	{
		ref_l->start_op = ref_r->start_op;
		ref_l->start    = ref_r->start;
	}

	if (ref_l->start && ref_l->start_op->id == '=')
		return true;

	return false;
}

hot static inline void
plan_and_merge_stop(Key* key, AstPlan* l, AstPlan *r)
{
	auto ref_l = &l->keys[key->order];
	auto ref_r = &r->keys[key->order];

	if (ref_l->stop && ref_r->stop)
	{
		// update key if it is < then the previous one
		if (plan_compare_ast(ref_l->stop, ref_r->stop) > 0)
		{
			ref_l->stop_op = ref_r->stop_op;
			ref_l->stop    = ref_r->stop;
		}
	} else
	if (ref_r->stop)
	{
		ref_l->stop_op = ref_r->stop_op;
		ref_l->stop    = ref_r->stop;
	}
}

hot static inline AstPlan*
plan_and_merge(Target* target, AstPlan* l, AstPlan *r)
{
	// merge r keys into l
	int matched_eq = 0;
	auto def = table_def(target->table);
	for (auto key = def->key; key; key = key->next)
	{
		if (plan_and_merge_start(key, l, r))
			matched_eq++;
		plan_and_merge_stop(key, l, r);
	}

	// SCAN_LOOKUP
	if (matched_eq == def->key_count)
		l->type = SCAN_LOOKUP;

	return l;
}

hot AstPlan*
plan(TargetList* target_list, Target* target, Ast* expr)
{
	auto def = table_def(target->table);
	if (expr == NULL)
		return ast_plan_allocate(def, SCAN);

	switch (expr->id) {
	case KAND:
	{
		auto l = plan(target_list, target, expr->l);
		auto r = plan(target_list, target, expr->r);
		return plan_and_merge(target, l, r);
	}

	case KOR:
	{
		// todo:
		break;
	}

	case KGTE:
	case KLTE:
	case '>':
	case '<':
	case '=':
	{
		Ast* path, *value;
		if (! plan_op_of(expr, &path, &value))
			break;
		auto plan = plan_op(target_list, target, expr, path, value);
		if (plan)
			return plan;
		break;
	}

	default:
		break;
	}

	return ast_plan_allocate(def, SCAN);
}
