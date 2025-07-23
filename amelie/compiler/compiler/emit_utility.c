
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

static void
emit_alter_table(Compiler* self)
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
	op2(self, CDDL, offset, flags);
}

static void
emit_ddl(Compiler* self)
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
	case STMT_ALTER_TABLE:
	{
		emit_alter_table(self);
		return;
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
	op2(self, CDDL, offset, flags);
}

void
emit_utility(Compiler* self)
{
	auto stmt = compiler_stmt(self);
	auto data = &self->code_data->data;
	switch (stmt->id) {
	// system
	case STMT_SHOW:
	{
		auto arg = ast_show_of(stmt->ast);
		auto offset = buf_size(data);
		encode_string(data, &arg->section);
		encode_string(data, &arg->name);
		encode_string(data, &arg->schema);
		encode_bool(data, arg->extended);

		auto r = op2(self, CSHOW, rpin(self, TYPE_JSON), offset);
		offset = buf_size(data);
		encode_string(data, &arg->section);

		auto format = &arg->format;
		if (str_empty(format))
			format = self->parser.local->format;
		op3(self, CCONTENT_JSON, r, offset, (intptr_t)format);
		runpin(self, r);
		break;
	}
	case STMT_CHECKPOINT:
	{
		auto arg = ast_checkpoint_of(stmt->ast);
		int workers;
		if (arg->workers) {
			workers = arg->workers->integer;
		} else {
			workers = opt_int_of(&config()->checkpoint_workers);
			if (workers == 0)
				workers = 1;
		}
		op1(self, CCHECKPOINT, workers);
		break;
	}

	// user
	case STMT_CREATE_TOKEN:
	{
		auto arg = ast_token_create_of(stmt->ast);
		auto offset = buf_size(data);
		encode_string(data, &arg->user->string);
		if (! arg->expire)
		{
			Str str;
			str_set_cstr(&str, "1 year");
			encode_string(data, &str);
		}
		auto r = op2(self, CUSER_CREATE_TOKEN, rpin(self, TYPE_JSON), offset);
		offset = buf_size(data);
		encode_raw(data, "token", 5);
		op3(self, CCONTENT_JSON, r, offset, (intptr_t)self->parser.local->format);
		runpin(self, r);
		break;
	}
	case STMT_CREATE_USER:
	{
		auto arg = ast_user_create_of(stmt->ast);
		auto offset = buf_size(data);
		user_config_write(arg->config, data, true);
		op2(self, CUSER_CREATE, offset, arg->if_not_exists);
		break;
	}
	case STMT_DROP_USER:
	{
		auto arg = ast_user_drop_of(stmt->ast);
		auto offset = buf_size(data);
		encode_string(data, &arg->name->string);
		op2(self, CUSER_DROP, offset, arg->if_exists);
		break;
	}
	case STMT_ALTER_USER:
	{
		auto arg = ast_user_alter_of(stmt->ast);
		auto offset = buf_size(data);
		user_config_write(arg->config, data, true);
		op1(self, CUSER_ALTER, offset);
		break;
	}

	// replica
	case STMT_CREATE_REPLICA:
	{
		auto arg = ast_replica_create_of(stmt->ast);
		auto offset = buf_size(data);
		replica_config_write(arg->config, data);
		op2(self, CREPLICA_CREATE, offset, arg->if_not_exists);
		break;
	}
	case STMT_DROP_REPLICA:
	{
		auto arg = ast_replica_drop_of(stmt->ast);
		auto offset = buf_size(data);
		encode_string(data, &arg->id->string);
		op2(self, CREPLICA_DROP, offset, arg->if_exists);
		break;
	}

	// replication
	case STMT_START_REPL:
	{
		op0(self, CREPL_START);
		break;
	}
	case STMT_STOP_REPL:
	{
		op0(self, CREPL_STOP);
		break;
	}
	case STMT_SUBSCRIBE:
	{
		auto arg = ast_repl_subscribe_of(stmt->ast);
		auto offset = code_data_add_string(self->code_data, &arg->id->string);
		op1(self, CREPL_SUBSCRIBE, offset);
		break;
	}
	case STMT_UNSUBSCRIBE:
	{
		op0(self, CREPL_UNSUBSCRIBE);
		break;
	}

	// ddl
	default:	
		emit_ddl(self);
		break;
	}

	op0(self, CRET);
}
