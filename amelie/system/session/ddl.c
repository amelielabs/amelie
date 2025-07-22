
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
#include <amelie_os.h>
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
#include <amelie_catalog.h>
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
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>

static int
prepare_alter_table(Compiler* self, int* flags_ref)
{
	auto stmt   = compiler_stmt(self);
	auto data   = &self->code_data->data;
	auto arg    = ast_table_alter_of(stmt->ast);
	auto offset = 0;
	auto flags  = 0;
	switch (arg->type) {
	case TABLE_ALTER_RENAME:
	{
		offset = table_op_rename(data, &arg->schema, &arg->name,
		                         &arg->schema_new,
		                         &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_IDENTITY:
	{
		offset = table_op_set_identity(data, &arg->schema, &arg->name,
		                               arg->identity->integer);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_UNLOGGED:
	{
		offset = table_op_set_unlogged(data, &arg->schema, &arg->name,
		                               arg->unlogged);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_COLUMN_RENAME:
	{
		offset = table_op_column_rename(data, &arg->schema, &arg->name,
		                                &arg->column_name,
		                                &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_ADD:
	{
		offset = table_op_column_add(data, &arg->schema, &arg->name, arg->column);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_not_exists)
			flags |= DDL_IF_COLUMN_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_DROP:
	{
		offset = table_op_column_drop(data, &arg->schema, &arg->name, &arg->column_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_DEFAULT,
		                             &arg->schema, &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_IDENTITY:
	case TABLE_ALTER_COLUMN_UNSET_IDENTITY:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_IDENTITY,
		                             &arg->schema, &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_STORED:
	case TABLE_ALTER_COLUMN_UNSET_STORED:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_STORED,
		                             &arg->schema, &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_RESOLVED:
	case TABLE_ALTER_COLUMN_UNSET_RESOLVED:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_RESOLVED,
		                             &arg->schema, &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	default:
		abort();
		break;
	}

	*flags_ref = flags;
	return offset;
}

static int
prepare_ddl(Compiler* self, int* flags_ref)
{
	auto stmt   = compiler_stmt(self);
	auto data   = &self->code_data->data;
	auto offset = 0;
	auto flags  = 0;
	switch (stmt->id) {
	// schema
	case STMT_CREATE_SCHEMA:
	{
		auto arg = ast_schema_create_of(stmt->ast);
		offset = schema_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_SCHEMA:
	{
		auto arg = ast_schema_drop_of(stmt->ast);
		offset = schema_op_drop(data, &arg->name->string, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_SCHEMA:
	{
		auto arg = ast_schema_alter_of(stmt->ast);
		offset = schema_op_rename(data, &arg->name->string, &arg->name_new->string);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// table
	case STMT_CREATE_TABLE:
	{
		auto arg = ast_table_create_of(stmt->ast);
		offset = table_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_TABLE:
	{
		auto arg = ast_table_drop_of(stmt->ast);
		offset = table_op_drop(data, &arg->schema, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_TRUNCATE:
	{
		auto arg = ast_table_truncate_of(stmt->ast);
		offset = table_op_truncate(data, &arg->schema, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// index
	case STMT_CREATE_INDEX:
	{
		auto arg = ast_index_create_of(stmt->ast);
		offset = table_op_index_create(data, &arg->table_schema, &arg->table_name, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_INDEX:
	{
		auto arg = ast_index_drop_of(stmt->ast);
		offset = table_op_index_drop(data, &arg->table_schema, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_INDEX:
	{
		auto arg = ast_index_alter_of(stmt->ast);
		offset = table_op_index_rename(data, &arg->table_schema, &arg->table_name,
		                               &arg->name, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	default:
		abort();
		break;
	}

	*flags_ref = flags;
	return offset;
}

static void
session_execute_ddl_stmt(Session* self, Tr* tr)
{
	// prepare ddl operation
	auto compiler = &self->compiler;
	auto stmt     = compiler_stmt(compiler);
	int  offset   = 0;
	int  flags    = 0;
	if (stmt->id == STMT_ALTER_TABLE)
		offset = prepare_alter_table(compiler, &flags);
	else
		offset = prepare_ddl(compiler, &flags);

	// execute ddl operation
	auto pos = code_data_at(compiler->code_data, offset);
	auto wal_write = catalog_execute(&share()->db->catalog, tr, pos, flags);
	if (! wal_write)
		return;

	assert(! tr_read_only(tr));
	Write write;
	write_init(&write);
	defer(write_free, &write);
	write_begin(&write);
	write_add(&write, &tr->log.write_log);

	WriteList write_list;
	write_list_init(&write_list);
	write_list_add(&write_list, &write);
	wal_mgr_write(&share()->db->wal_mgr, &write_list);
}

void
session_execute_ddl(Session* self)
{
	// respect system read-only state
	if (opt_int_of(&state()->read_only))
		error("system is in read-only mode");

	// upgrade to exclusive lock
	session_lock(self, LOCK_EXCLUSIVE);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		session_execute_ddl_stmt(self, &tr);

		// commit
		tr_commit(&tr);
	);
	if (unlikely(on_error))
	{
		tr_abort(&tr);
		rethrow();
	}
}
