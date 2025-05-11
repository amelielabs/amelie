
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>

static Path*
path_allocate(Target* target, Keys* keys)
{
	Path* self;
	self = palloc(sizeof(Path) + sizeof(PathKey) * keys->list_count);
	self->type                = PATH_SCAN;
	self->target              = target;
	self->match_start         = 0;
	self->match_start_columns = 0;
	self->match_start_vars    = 0;
	self->match_stop          = 0;
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
path_ast_compare(Ast* a, Ast* b)
{
	switch (a->id) {
	case KINT:
	case KTIMESTAMP:
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	case KSTRING:
		return str_compare_fn(&a->string, &b->string);
	case KUUID:
	{
		Uuid l, r;
		uuid_set(&l, &a->string);
		uuid_set(&r, &b->string);
		return uuid_compare(&l, &r);
	}
	// KNAME
	// KNAME_COMPOUND
	}
	return 0;
}

static inline bool
path_key_compare(Path* self, PathKey* key, Ast* ast)
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
path_start(PathKey* self, Ast* op, Ast* value)
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
	if (path_ast_compare(self->start, value) == -1)
		return true;

	return false;
}

static inline bool
path_stop(PathKey* self, Ast* op, Ast* value)
{
	// match stop conditions only for outer keys
	if (self->key->order > 0)
		return false;

	unused(op);
	if (! self->stop)
		return true;

	// update key if it is < then the previous one
	if (path_ast_compare(self->stop, value) > 0)
		return true;

	return false;
}

static inline Column*
path_column(Path* self, Str* string)
{
	// match outer target [target.]name and find the column
	Str name;
	str_split(string, &name, '.');

	Str path;
	str_init(&path);
	str_set_str(&path, string);

	// search outer targets (excluding self)
	auto target = self->target->prev;
	while (target)
	{
		if (str_compare(&target->name, &name))
			break;
		target = target->prev;
	}
	if (! target)
		target = targets_match_outer(self->target->targets->outer, &name);
	if (! target)
		return NULL;

	// exclude target name from the path
	str_advance(&path, str_size(&name) + 1);

	// exclude nested path (target.column.path)
	if (str_split(&path, &name, '.'))
		str_advance(&path, str_size(&name) + 1);
	else
		str_advance(&path, str_size(&name));

	Column* column = NULL;
	auto cte = target->from_cte;
	if (target->type == TARGET_CTE && cte->cte->args.count > 0)
	{
		// find column in the CTE arguments list, redirect to the CTE statement
		auto arg = columns_find(&cte->cte->args, &name);
		if (arg)
			column = columns_find_by(target->from_columns, arg->order);
	} else
	{
		// find unique column name in the target
		bool column_conflict = false;
		column = columns_find_noconflict(target->from_columns, &name, &column_conflict);
	}

	return column;
}

static inline void
path_key(Path* self, Scope* scope, PathKey* key, AstList* ops)
{
	auto node = ops->list;
	for (; node; node = node->next)
	{
		// match the key against the expression
		auto op = node->ast;
		Ast* value;
		if (path_key_compare(self, key, op->l))
			value = op->r;
		else
		if (path_key_compare(self, key, op->r))
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
		case KUUID:
			if (unlikely(column->type != TYPE_UUID))
				continue;
			break;
		case KNAME:
		{
			// match variable by name
			auto match = vars_find(&scope->vars, &value->string);
			if (! match)
				continue;
			break;
		}
		case KNAME_COMPOUND:
		{
			// match outer target [target.]column and find the column
			auto match = path_column(self, &value->string);
			if (! match)
				continue;
			if (column->type != match->type)
				continue;
			// todo: json var support
			break;
		}
		}

		// apply equ or range operation to the key
		switch (op->id) {
		case '>':
		case KGTE:
		case '=':
			if (! path_start(key, op, value))
				break;
			key->start_op = op;
			key->start = value;
			break;
		case '<':
		case KLTE:
			if (! path_stop(key, op, value))
				break;
			key->stop_op = op;
			key->stop = value;
			break;
		}
	}
}

Path*
path_create(Target* target, Scope* scope, Keys* keys, AstList* ops)
{
	auto self = path_allocate(target, keys);
	auto match_eq = 0;
	auto match_last_start = -1;
	auto match_last_stop  = -1;
	list_foreach(&keys->list)
	{
		auto key = list_at(Key, link);
		auto key_path = &self->keys[key->order];
		path_key(self, scope, key_path, ops);

		// count sequential number of matches from start
		if (key_path->start && (match_last_start == (key->order - 1)))
		{
			match_last_start = key->order;
			self->match_start++;
			if (key_path->start->id == KNAME_COMPOUND)
				self->match_start_columns++;
			if (key_path->start->id == KNAME)
				self->match_start_vars++;
			if (key_path->start_op->id == '=')
				match_eq++;
		}
		if (key_path->stop && (match_last_stop == (key->order - 1)))
		{
			match_last_stop = key->order;
			self->match_stop++;
		}
	}

	// point lookup
	if (match_eq == keys->list_count)
		self->type = PATH_LOOKUP;

	return self;
}

uint32_t
path_create_hash(Path* self)
{
	assert(self->type == PATH_LOOKUP);
	Uuid     uuid;
	uint32_t hash = 0;
	for (auto i = 0; i < self->match_start; i++)
	{
		auto    value = self->keys[i].start;
		void*   data;
		int     data_size;
		int32_t integer_32;
		if (value->id == KINT || value->id == KTIMESTAMP)
		{
			if (self->keys[i].key->column->type_size == 4)
			{
				integer_32 = value->integer;
				data = &integer_32;
				data_size = sizeof(integer_32);
			} else
			{
				data = &value->integer;
				data_size = sizeof(value->integer);
			}
		} else
		if (value->id == KUUID)
		{
			uuid_set(&uuid, &value->string);
			data = &uuid;
			data_size = sizeof(uuid);
		} else
		{
			data = str_u8(&value->string);
			data_size = str_size(&value->string);
		}
		hash = hash_murmur3_32(data, data_size, hash);
	}
	return hash;
}
