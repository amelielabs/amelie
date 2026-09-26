
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
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>

static void
copy_object(Parser* self, Columns* columns, Set* values, uint8_t** pos)
{
	auto row = set_reserve(values);

	auto buf = buf_create();
	defer_buf(buf);
	buf_reserve(buf, sizeof(bool) * columns->count);
	memset(buf->start, 0, sizeof(bool) * columns->count);

	auto match = (bool*)buf->start;
	auto match_count = 0;

	// {}
	unpack_obj(pos);
	while (! unpack_obj_end(pos))
	{
		Str name;
		unpack_str(pos, &name);

		// match column
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '{str}': does not exists", &name);

		// ensure column is not redefined
		if (unlikely(match[column->order]))
			error("column '{str}': value is redefined", &name);
		match[column->order] = true;
		match_count++;

		// parse column value
		auto column_value = &row[column->order];
		parse_value_data(self->local, column, column_value, pos);
		parse_value_validate(NULL, column, column_value, NULL);

	}
	if (match_count == columns->count)
		return;

	// default value, write IDENTITY, DEFAULT
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
copy_args(Parser* self, Columns* columns, Set* values, uint8_t* args)
{
	// {}
	auto pos = args;
	if (data_is_obj(pos))
		return copy_object(self, columns, values, &pos);

	// [{}, ...]
	if (data_is_array(pos))
	{
		unpack_array(&pos);
		while (! unpack_array_end(&pos))
		{
			if (unlikely(! data_is_obj(pos)))
				error("write: {{}} expected");
			copy_object(self, columns, values, &pos);
		}
		return;
	}

	// error
	error("write: expected {{}} or []");
}

static void
copy_insert(Parser* self, Table* table, Clone* clone, uint8_t* args)
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

	// set timeline
	Timeline* timeline;
	if (clone)
		timeline = &clone->config->timeline;
	else
		timeline = table_main(table);

	auto columns = table_columns(table);
	auto target  = target_allocate();
	target->type          = TARGET_TABLE;
	target->ast           = stmt->ast;
	target->from_lock     = LOCK_SHARED_RW;
	target->from_table    = table;
	target->from_timeline = timeline;
	target->columns       = columns;
	str_set_str(&target->name, &table->config->name);
	from_add(&insert->from, target);

	// add table/clone to the access list
	if (clone)
	{
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_SELECT);
		access_add(&self->program->access, &clone->rel, LOCK_NONE, PERM_INSERT);
	} else {
		access_add(&self->program->access, &table->rel, LOCK_SHARED_RW, PERM_INSERT);
	}

	// prepare result set
	insert->values = set_cache_create(self->set_cache, &self->program->sets);
	set_prepare(insert->values, columns->count, 0, NULL);

	// parse and set values
	copy_args(self, columns, insert->values, args);
}

static void
copy_publish(Parser* self, Channel* channel, uint8_t* args)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare execute stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_PUBLISH;
	stmt->ast = &ast_publish_allocate(block)->ast;
	stmt->is_return = true;

	// prepare arguments
	auto publish = ast_publish_of(stmt->ast);
	publish->channel = channel;
	publish->values  = set_cache_create(self->set_cache, &self->program->sets);
	set_prepare(publish->values, 1, 0, NULL);

	access_add(&self->program->access, &channel->rel,
	           LOCK_SHARED_RW, PERM_PUBLISH);

	// parse arguments
	auto pos = args;
	if (data_is_array(pos))
	{
		unpack_array(&pos);
		while (! unpack_array_end(&pos))
		{
			auto row = set_reserve(publish->values);
			auto size = data_sizeof(pos);
			value_set_json(row, pos, size, NULL);
			pos += size;
		}
	} else
	{
		auto row = set_reserve(publish->values);
		auto size = data_sizeof(pos);
		value_set_json(row, pos, size, NULL);
	}
}

static void
copy_execute(Parser* self, Udf* udf, uint8_t* args)
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

	// {} by default
	uint8_t args_empty[2] = {DATA_OBJ, DATA_OBJ_END};
	if (! args)
		args = args_empty;

	// parse arguments
	copy_args(self, &udf->config->args, execute->args, args);

	// ensure not a batch execution
	if (execute->args->count_rows > 1)
		error("batch function execution is not support");

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
parse_copy_api(Parser*  self, Program* program,
               Str*     rel_user,
               Str*     rel,
               uint8_t* args,
               bool     execute)
{
	Str* user = rel_user;
	if (str_empty(rel_user))
		user = &self->local->user;
	self->program = program;

	auto ref = catalog_find(&share()->db->catalog, REL_UNDEF, user, rel, true);
	if (execute) {
		if (ref->type != REL_UDF)
			error("relation {str}.{str} is not a function",
			      ref->user, ref->name);
	}

	switch (ref->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref);
		copy_insert(self, table, NULL, args);
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(ref);
		copy_insert(self, clone->table, clone, args);
		break;
	}
	case REL_CHANNEL:
	{
		auto channel = channel_of(ref);
		copy_publish(self, channel, args);
		break;
	}
	case REL_UDF:
	{
		auto udf = udf_of(ref);
		copy_execute(self, udf, args);
		break;
	}
	default:
	{
		error("relation '{str}': unsupported relation", rel);
		break;
	}
	}
}

void
parse_copy(Parser* self, Program* program,
           Str*    rel_user,
           Str*    rel,
           Str*    content_type,
           Str*    content)
{
	// csv
	if (str_is(content_type, "text/csv", 8))
		return parse_copy_csv(self, program, rel_user, rel, content);

	// application/json
	assert(str_empty(content_type));

	Str* user = rel_user;
	if (str_empty(rel_user))
		user = &self->local->user;
	self->program = program;

	// find relation
	auto ref = catalog_find(&share()->db->catalog, REL_UNDEF, user, rel, true);

	// only function call have empty content
	if (str_empty(content) && ref->type != REL_UDF)
		error("relation {str}.{str} is not a function",
		      ref->user, ref->name);

	auto args = str_u8(content);
	switch (ref->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref);
		copy_insert(self, table, NULL, args);
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(ref);
		copy_insert(self, clone->table, clone, args);
		break;
	}
	case REL_CHANNEL:
	{
		auto channel = channel_of(ref);
		copy_publish(self, channel, args);
		break;
	}
	case REL_UDF:
	{
		auto udf = udf_of(ref);
		copy_execute(self, udf, args);
		break;
	}
	default:
	{
		error("relation '{str}': unsupported relation", rel);
		break;
	}
	}
}
