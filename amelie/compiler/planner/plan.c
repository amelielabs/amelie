
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
#include <amelie_json.h>
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
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>

static Plan*
plan_allocate(Target* target, Keys* keys)
{
	Plan* self;
	self = palloc(sizeof(Plan) + sizeof(PlanKey) * keys->list_count);
	self->type        = PLAN_SCAN;
	self->target      = target;
	self->match_start = 0;
	self->match_stop  = 0;
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
	return self;
}

hot static inline int
plan_ast_compare(Ast* a, Ast* b)
{
	switch (a->id) {
	case KINT:
	case KTIMESTAMP:
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	case KSTRING:
		return str_compare_fn(&a->string, &b->string);
	// KNAME
	// KNAME_COMPOUND
	}
	return 0;
}

static inline bool
plan_key_compare(Plan* self, PlanKey* key, Ast* ast)
{
	// column
	auto column = key->key->column;
	if (ast->id == KNAME)
		return str_compare(&ast->string, &column->name);

	// [target.]column
	if (ast->id != KNAME_COMPOUND)
		return false;

	Str path;
	str_set_str(&path, &ast->string);
	Str name;
	str_split(&ast->string, &name, '.');

	// [target.]
	if (! str_compare(&self->target->name, &name))
		return false;

	str_advance(&path, str_size(&name) + 1);

	// skip target name
	str_split(&path, &name, '.');
	return str_compare(&name, &column->name);
}

static inline bool
plan_start(PlanKey* self, Ast* op, Ast* value)
{
	if (! self->start)
		return true;

	// do not redefine first point lookup
	if (self->start_op->id == '=')
		return false;

	// force as point lookup
	if (op->id == '=')
		return true;

	// update key if it is > then the previous one
	if (plan_ast_compare(self->start, value) == -1)
		return true;

	return false;
}

static inline bool
plan_stop(PlanKey* self, Ast* op, Ast* value)
{
	// match stop conditions only for outer keys
	if (self->key->order > 0)
		return false;

	unused(op);
	if (! self->stop)
		return true;

	// update key if it is < then the previous one
	if (plan_ast_compare(self->stop, value) > 0)
		return true;

	return false;
}

static inline void
plan_key(Plan* self, PlanKey* key, AstList* ops)
{
	auto node = ops->list;
	for (; node; node = node->next)
	{
		// match the key against the expression
		auto op = node->ast;
		Ast* value;
		if (plan_key_compare(self, key, op->l))
			value = op->r;
		else
		if (plan_key_compare(self, key, op->r))
			value = op->l;
		else
			continue;

		// validate value according to the key type
		auto column = key->key->column;
		switch (value->id) {
		case KINT:
			if (unlikely(column->type != TYPE_INT))
				continue;
			break;
		case KSTRING:
			if (unlikely(column->type != TYPE_STRING))
				continue;
			break;
		case KTIMESTAMP:
			if (unlikely(column->type != TYPE_TIMESTAMP))
				continue;
			break;
		case KNAME:
		case KNAME_COMPOUND:
			// todo: match and exclude inner targets
			// todo: compare column types
			continue;	
		}

		// apply equ or range operation to the key
		switch (op->id) {
		case '>':
		case KGTE:
		case '=':
			if (! plan_start(key, op, value))
				break;
			key->start_op = op;
			key->start = value;
			break;
		case '<':
		case KLTE:
			if (! plan_stop(key, op, value))
				break;
			key->stop_op = op;
			key->stop = value;
			break;
		}
	}
}

Plan*
plan_create(Target* target, Keys* keys, AstList* ops)
{
	auto self = plan_allocate(target, keys);
	auto match_eq = 0;
	list_foreach(&keys->list)
	{
		auto key = list_at(Key, link);
		auto key_plan = &self->keys[key->order];
		plan_key(self, key_plan, ops);
		if (key_plan->start)
		{
			self->match_start++;
			if (key_plan->start_op->id == '=')
				match_eq++;
		}
		if (key_plan->stop)
			self->match_stop++;
	}

	// point lookup
	if (match_eq == keys->list_count)
		self->type = PLAN_LOOKUP;

	return self;
}
