
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>

static void
import_object(Parser* self, Columns* columns, Set* values, uint8_t** pos)
{
	auto row = set_reserve(values);

	auto buf = buf_create();
	defer_buf(buf);
	buf_reserve(buf, sizeof(bool) * columns->count);
	memset(buf->start, 0, sizeof(bool) * columns->count);

	auto match = (bool*)buf->start;
	auto match_count = 0;

	// {}
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		Str name;
		json_read_string(pos, &name);

		// match column
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '%.*s': does not exists", str_size(&name),
			      str_of(&name));

		// ensure column is not redefined
		if (unlikely(match[column->order]))
			error("column '%.*s': value is redefined", str_size(&name),
			      str_of(&name));
		match[column->order] = true;
		match_count++;

		// parse column value
		auto column_value = &row[column->order];
		parse_value_decode(self->local, column, column_value, pos);
		parse_value_validate(NULL, column, column_value, NULL);

	}
	if (match_count == columns->count)
		return;

	// default value, write IDENTITY, RANDOM or DEFAULT
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (match[column->order])
			continue;
		auto column_value = &row[column->order];
		parse_value_default(column, column_value);
		parse_value_validate(NULL, column, column_value, NULL);
	}
}

static void
import_args(Parser* self, Columns* columns, Set* values, uint8_t* args)
{
	// {}
	auto pos = args;
	if (json_is_obj(pos))
		return import_object(self, columns, values, &pos);

	// [ {}, ... ]
	if (json_is_array(pos))
	{
		json_read_array(&pos);
		while (! json_read_array_end(&pos))
		{
			if (unlikely(! json_is_obj(pos)))
				error("write: {} expected");
			import_object(self, columns, values, &pos);
		}
		return;
	}

	// error
	error("write: expected {} or []");
}

static void
import_insert(Parser* self, Table* table, Branch* branch, uint8_t* args)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare insert stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_INSERT;
	stmt->ast = &ast_insert_allocate(block)->ast;
	stmt->is_return = true;

	// create insert target
	auto insert  = ast_insert_of(stmt->ast);
	stmt->ret = &insert->ret;

	// set snapshot
	Snapshot* snapshot;
	if (branch)
		snapshot = &branch->config->snapshot;
	else
		snapshot = table_main(table);

	auto columns = table_columns(table);
	auto target  = target_allocate();
	target->type          = TARGET_TABLE;
	target->ast           = stmt->ast;
	target->from_lock     = LOCK_SHARED_RW;
	target->from_table    = table;
	target->from_snapshot = snapshot;
	target->columns       = columns;
	str_set_str(&target->name, &table->config->name);
	from_add(&insert->from, target);

	// add table/branch to the access list
	if (branch)
	{
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_SELECT);
		access_add(&self->program->access, &branch->rel, LOCK_NONE, PERM_INSERT);
	} else {
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_INSERT);
	}

	// prepare result set
	insert->values = set_cache_create(self->set_cache, &self->program->sets);
	set_prepare(insert->values, columns->count, 0, NULL);

	// parse and set values
	import_args(self, columns, insert->values, args);

	// create a list of generated columns expressions
	if (columns->count_stored > 0)
		parse_generated(stmt);
	
	// if table has resolved columns, handle insert as upsert
	// and apply the resolve expressions on conflicts
	if (columns->count_resolved > 0)
		parse_resolved(stmt);

	// update requested permissions to UPDATE
	if (insert->on_conflict == ON_CONFLICT_UPDATE)
	{
		auto access = access_find(&self->program->access,
		                          &table->config->user,
		                          &table->config->name);
		access->perm |= PERM_UPDATE;
	}
}

static void
import_execute(Parser* self, Udf* udf, uint8_t* args)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare execute stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_EXECUTE;
	stmt->ast = &ast_execute_allocate()->ast;
	stmt->is_return = true;

	auto execute = ast_execute_of(stmt->ast);
	stmt->ret = &execute->ret;

	// prepare arguments
	execute->udf  = udf;
	execute->args = set_cache_create(self->set_cache, &self->program->sets);
	set_prepare(execute->args, udf->config->args.count, 0, NULL);

	// parse arguments
	import_args(self, &udf->config->args, execute->args, args);

	// set returning column
	if (udf->config->type != TYPE_NULL)
	{
		auto column = column_allocate();
		column_set_name(column, &udf->config->name);
		column_set_type(column, udf->config->type, -1);
		columns_add(&execute->ret.columns, column);
	}
}

void
parse_import(Parser*  self, Program* program,
             Str*     rel_user,
             Str*     rel,
             uint8_t* args)
{
	Str* user = rel_user;
	if (str_empty(rel_user))
		user = &self->local->user;

	self->program = program;
	auto ref = catalog_find(&share()->db->catalog, user, rel, true);
	switch (ref->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref);
		import_insert(self, table, NULL, args);
		break;
	}
	case REL_BRANCH:
	{
		auto branch = branch_of(ref);
		import_insert(self, branch->table, branch, args);
		break;
	}
	case REL_TOPIC:
	{
		break;
	}
	case REL_UDF:
	{
		auto udf = udf_of(ref);
		import_execute(self, udf, args);
		break;
	}
	default:
	{
		error("relation '%.*s': unsupported relation", str_size(rel),
		      str_of(rel));
		break;
	}
	}
}
