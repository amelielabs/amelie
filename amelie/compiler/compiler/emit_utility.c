
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

static void
emit_alter_table(Compiler* self)
{
	auto stmt   = compiler_stmt(self);
	auto db     = self->parser.db;
	auto data   = &self->code_data->data;
	auto arg    = ast_table_alter_of(stmt->ast);
	auto offset = 0;
	auto flags  = 0;
	switch (arg->type) {
	case TABLE_ALTER_RENAME:
	{
		offset = table_op_rename(data, db, &arg->name, db, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_IDENTITY:
	{
		offset = table_op_set_identity(data, db, &arg->name, arg->identity->integer);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_UNLOGGED:
	{
		offset = table_op_set_unlogged(data, db, &arg->name, arg->unlogged);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_COLUMN_RENAME:
	{
		offset = table_op_column_rename(data, db, &arg->name, &arg->column_name,
		                                &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_ADD:
	{
		offset = table_op_column_add(data, db, &arg->name, arg->column);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_not_exists)
			flags |= DDL_IF_COLUMN_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_DROP:
	{
		offset = table_op_column_drop(data, db, &arg->name, &arg->column_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_DEFAULT,
		                             db, &arg->name,
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
		                             db, &arg->name,
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
		                             db, &arg->name,
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
	auto db     = self->parser.db;
	auto data   = &self->code_data->data;
	auto offset = 0;
	auto flags  = 0;
	switch (stmt->id) {
	// database
	case STMT_CREATE_DB:
	{
		auto arg = ast_db_create_of(stmt->ast);
		offset = db_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_DB:
	{
		auto arg = ast_db_drop_of(stmt->ast);
		offset = db_op_drop(data, &arg->name->string, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_DB:
	{
		auto arg = ast_db_alter_of(stmt->ast);
		offset = db_op_rename(data, &arg->name->string, &arg->name_new->string);
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
		offset = table_op_drop(data, db, &arg->name);
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
		offset = table_op_truncate(data, db, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// index
	case STMT_CREATE_INDEX:
	{
		auto arg = ast_index_create_of(stmt->ast);
		offset = table_op_index_create(data, db, &arg->table_name, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_INDEX:
	{
		auto arg = ast_index_drop_of(stmt->ast);
		offset = table_op_index_drop(data, db, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_INDEX:
	{
		auto arg = ast_index_alter_of(stmt->ast);
		offset = table_op_index_rename(data, db, &arg->table_name, &arg->name,
		                               &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// function
	case STMT_CREATE_FUNCTION:
	{
		auto arg = ast_function_create_of(stmt->ast);
		if (arg->or_replace)
			offset = udf_op_replace(data, arg->config);
		else
			offset = udf_op_create(data, arg->config);
		flags = 0;
		break;
	}
	case STMT_DROP_FUNCTION:
	{
		auto arg = ast_function_drop_of(stmt->ast);
		offset = udf_op_drop(data, db, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_FUNCTION:
	{
		auto arg = ast_function_alter_of(stmt->ast);
		offset = udf_op_rename(data, db, &arg->name, db, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	default:
		abort();
		break;
	}
	op2(self, CDDL, offset, flags);
}

static int
emit_show(Compiler* self)
{
	auto stmt = compiler_stmt(self);
	auto arg  = ast_show_of(stmt->ast);

	// find show() function
	Str name;
	str_set(&name, "show", 4);
	auto fn = function_mgr_find(share()->function_mgr, &name);
	assert(fn);

	// show(section[, name, extended])
	auto argc = 1;
	auto r = emit_string(self, &arg->section, false);
	op1(self, CPUSH, r);
	runpin(self, r);
	if (! str_empty(&arg->name))
	{
		r = emit_string(self, &arg->name, false);
		op1(self, CPUSH, r);
		runpin(self, r);
		argc++;
	}
	if (arg->extended)
	{
		r = op2pin(self, CBOOL, TYPE_BOOL, arg->extended);
		op1(self, CPUSH, r);
		runpin(self, r);
		argc++;
	}
	r = op4pin(self, CCALL, fn->type, (intptr_t)fn, argc, -1);
	return r;
}

void
emit_utility(Compiler* self)
{
	auto stmt    = compiler_stmt(self);
	auto data    = &self->code_data->data;
	auto program = self->program;

	// explicily set program to have exclusive lock for majority of
	// the utility/ddl commands with some exceptions below
	program->lock = LOCK_EXCLUSIVE;
	program->utility = true;

	int r = -1;
	switch (stmt->id) {
	// system
	case STMT_SHOW:
	{
		r = emit_show(self);

		// shared lock
		program->lock = LOCK_SHARED;
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

		// checkpoint operation must not hold any locks
		program->lock = LOCK_NONE;
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
		r = op2pin(self, CUSER_CREATE_TOKEN, TYPE_JSON, offset);

		// shared lock
		program->lock = LOCK_SHARED;
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
	{
		emit_ddl(self);
		break;
	}
	}

	// CRET
	op1(self, CRET, r);
	if (r != -1)
		runpin(self, r);
}
