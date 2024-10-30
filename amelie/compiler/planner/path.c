
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>

static inline AstPath*
ast_path_allocate(Keys* keys, int type)
{
	AstPath* self;
	self = ast_allocate(0, sizeof(AstPath));
	self->type       = type;
	self->match      = 0;
	self->match_stop = 0;
	self->keys       = (AstPathKey*)palloc(sizeof(AstPathKey) * keys->list_count);
	list_foreach(&keys->list)
	{
		auto key = list_at(Key, link);
		auto ref = &self->keys[key->order];
		ref->key      = key;
		ref->start_op = NULL;
		ref->start    = NULL;
		ref->stop_op  = NULL;
		ref->stop     = NULL;
	}
	self->list = NULL;
	self->next = NULL;
	return self;
}

hot static inline int
path_compare_ast(Ast* a, Ast* b)
{
	if (a->id == KINT || a->id == KTIMESTAMP)
	{
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	}
	if (a->id == KSTRING)
		return str_compare_fn(&a->string, &b->string);
	return 0;
}

static inline bool
path_compare(Path* self, Ast* ast, Key* key)
{
	auto column = key->column;

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
	str_split(&ast->string, &name, '.');

	// [target.]
	if (target_compare(self->target, &name))
	{
		str_advance(&path, str_size(&name) + 1);

		// skip target name
		str_split(&path, &name, '.');
	}

	// column[.path]
	bool compound = str_split(&path, &name, '.');
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
path_key_is(Path* self, Key* key, Ast* path, Ast* value)
{
	// compare path with column key
	if (! path_compare(self, path, key))
		return false;

	// validate value to key type
	switch (value->id) {
	case KINT:
		if (unlikely(key->type != TYPE_INT))
			return false;
		break;
	case KSTRING:
		if (unlikely(key->type != TYPE_STRING))
			return false;
		break;
	case KTIMESTAMP:
		if (unlikely(key->type != TYPE_TIMESTAMP))
			return false;
		break;
	}

	// join: name = name

	// do not match keys for outer targets
	auto target = self->target;
	if (value->id == KNAME_COMPOUND)
	{
		Str name;
		str_split(&path->string, &name, '.');

		auto join = target_list_match(self->target_list, &name);
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
path_key_find(Path* self, Ast* path, Ast* value)
{
	list_foreach(&self->keys->list)
	{
		auto key = list_at(Key, link);
		if (path_key_is(self, key, path, value))
			return key;
	}
	return NULL;
}

hot static inline AstPath*
path_op(Path* self, Ast* op, Ast* path, Ast* value)
{
	// find matching key
	auto key = path_key_find(self, path, value);
	if (! key)
		return NULL;

	auto ast     = ast_path_allocate(self->keys, PATH_SCAN);
	auto ast_key = &ast->keys[key->order];
	switch (op->id) {
		break;
	case '>':
	case KGTE:
	case '=':
	{
		ast_key->start_op = op;
		ast_key->start    = value;
		if (op->id == '=')
		{
			if (self->keys->list_count == 1)
				ast->type = PATH_LOOKUP;
		}
		break;
	}
	case '<':
	case KLTE:
	{
		// stop condition works only on the outer key
		if (key->order > 0)
			break;
		ast_key->stop_op = op;
		ast_key->stop    = value;
		break;
	}
	}

	return ast;
}

hot static inline bool
path_op_of(Ast* expr, Ast** path, Ast** value)
{
	//
	// name = expr
	// expr = name
	//
	if (expr->l->id == KNAME || expr->l->id == KNAME_COMPOUND)
	{
		if (expr->r->id == KINT ||
		    expr->r->id == KSTRING ||
		    expr->r->id == KTIMESTAMP)
		{
			*path  = expr->l;
			*value = expr->r;
			return true;
		}
	} else
	if (expr->r->id == KNAME || expr->r->id == KNAME_COMPOUND)
	{
		if (expr->l->id == KINT ||
		    expr->l->id == KSTRING ||
		    expr->l->id == KTIMESTAMP)
		{
			*path  = expr->r;
			*value = expr->l;
			return true;
		}
	}
	return false;
}

hot static inline bool
path_merge_start(Key* key, AstPath* l, AstPath *r)
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
				if (path_compare_ast(ref_l->start, ref_r->start) == -1)
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
path_merge_stop(Key* key, AstPath* l, AstPath *r)
{
	auto ref_l = &l->keys[key->order];
	auto ref_r = &r->keys[key->order];

	if (ref_l->stop && ref_r->stop)
	{
		// update key if it is < then the previous one
		if (path_compare_ast(ref_l->stop, ref_r->stop) > 0)
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

hot static inline AstPath*
path_merge(Path* self, AstPath* l, AstPath *r)
{
	// merge r keys into l
	int matched_eq = 0;
	auto keys = self->keys;
	list_foreach(&keys->list)
	{
		auto key = list_at(Key, link);
		if (path_merge_start(key, l, r))
			matched_eq++;
		path_merge_stop(key, l, r);
	}

	// point lookup
	if (matched_eq == keys->list_count)
		l->type = PATH_LOOKUP;

	return l;
}

hot static AstPath*
path_expr(Path* self, Ast* expr);

hot static inline AstPath*
path_between(Path* self, Ast* expr)
{
	// NOT BETWEEN
	if (! expr->integer)
		return NULL;

	//    . BETWEEN .
	// expr       . AND .
	//            x     y
	if (expr->l->id != KNAME & expr->l->id != KNAME_COMPOUND)
		return NULL;

	auto x = expr->r->l;
	auto y = expr->r->r;

	// expr >= x AND expr <= y
	auto gte = ast(KGTE);
	gte->l = expr->l;
	gte->r = x;

	auto lte = ast(KLTE);
	lte->l = expr->l;
	lte->r = y;

	auto and = ast(KAND);
	and->l = gte;
	and->r = lte;
	return path_expr(self, and);
}

hot static AstPath*
path_expr(Path* self, Ast* expr)
{
	auto keys = self->keys;
	if (expr == NULL)
		return ast_path_allocate(keys, PATH_SCAN);

	switch (expr->id) {
	case KAND:
	{
		auto l = path_expr(self, expr->l);
		auto r = path_expr(self, expr->r);
		return path_merge(self, l, r);
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
		if (! path_op_of(expr, &path, &value))
			break;
		auto op = path_op(self, expr, path, value);
		if (op)
			return op;
		break;
	}
	case KBETWEEN:
	{
		auto op = path_between(self, expr);
		if (op)
			return op;
		break;
	}
	default:
		break;
	}

	return ast_path_allocate(keys, PATH_SCAN);
}

hot AstPath*
path_create(Path* self, Ast* expr)
{
	auto result = path_expr(self, expr);

	// calculate number of key matches from start
	bool match_start = true;
	list_foreach(&self->keys->list)
	{
		auto key = list_at(Key, link);
		auto ref = &result->keys[key->order];
		if (match_start && ref->start)
			result->match++;
		else
			match_start = false;
		if (ref->stop)
			result->match_stop++;
	}

	return result;
}
