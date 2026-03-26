
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
#include <amelie_plan.h>
#include <amelie_compiler.h>

static void
emit_alter_table(Compiler* self)
{
	auto stmt   = compiler_stmt(self);
	auto user   = self->parser.user;
	auto data   = &self->code_data->data;
	auto arg    = ast_table_alter_of(stmt->ast);
	auto offset = 0;
	auto flags  = 0;
	switch (arg->type) {
	case TABLE_ALTER_RENAME:
	{
		offset = table_op_rename(data, user, &arg->name, user, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_IDENTITY:
	{
		offset = table_op_set_identity(data, user, &arg->name, arg->identity->integer);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_SET_UNLOGGED:
	{
		offset = table_op_set_unlogged(data, user, &arg->name, arg->unlogged);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_COLUMN_RENAME:
	{
		offset = table_op_column_rename(data, user, &arg->name, &arg->column_name,
		                                &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_ADD:
	{
		offset = table_op_column_add(data, user, &arg->name, arg->column);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_not_exists)
			flags |= DDL_IF_COLUMN_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_DROP:
	{
		offset = table_op_column_drop(data, user, &arg->name, &arg->column_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_DEFAULT,
		                             user, &arg->name,
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
		                             user, &arg->name,
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
		                             user, &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_STORAGE_ADD:
	{
		offset = table_op_storage_add(data, user, &arg->name, arg->volume);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_storage_not_exists)
			flags |= DDL_IF_STORAGE_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_STORAGE_DROP:
	{
		offset = table_op_storage_drop(data, user, &arg->name, &arg->storage_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_storage_exists)
			flags |= DDL_IF_STORAGE_EXISTS;
		break;
	}
	case TABLE_ALTER_STORAGE_PAUSE:
	{
		offset = table_op_storage_pause(data, user, &arg->name, &arg->storage_name, true);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_storage_exists)
			flags |= DDL_IF_STORAGE_EXISTS;
		break;
	}
	case TABLE_ALTER_STORAGE_RESUME:
	{
		offset = table_op_storage_pause(data, user, &arg->name, &arg->storage_name, false);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_storage_exists)
			flags |= DDL_IF_STORAGE_EXISTS;
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
	auto user     = self->parser.user;
	auto data   = &self->code_data->data;
	auto offset = 0;
	auto flags  = 0;
	switch (stmt->id) {
	// storage
	case STMT_CREATE_STORAGE:
	{
		auto arg = ast_storage_create_of(stmt->ast);
		offset = storage_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_STORAGE:
	{
		auto arg = ast_storage_drop_of(stmt->ast);
		offset = storage_op_drop(data, &arg->name->string);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_STORAGE:
	{
		auto arg = ast_storage_alter_of(stmt->ast);
		offset = storage_op_rename(data, &arg->name->string, &arg->name_new->string);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// user
	case STMT_CREATE_USER:
	{
		auto arg = ast_user_create_of(stmt->ast);
		offset = user_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_USER:
	{
		auto arg = ast_user_drop_of(stmt->ast);
		offset = user_op_drop(data, &arg->name->string, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_USER:
	{
		auto arg = ast_user_alter_of(stmt->ast);
		if (arg->type == USER_ALTER_RENAME)
			offset = user_op_rename(data, &arg->name->string, &arg->name_new->string);
		else
		if (arg->type == USER_ALTER_REVOKE)
			offset = user_op_revoke(data, &arg->name->string, &arg->revoked_at);
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
		offset = table_op_drop(data, user, &arg->name);
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
		offset = table_op_truncate(data, user, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// index
	case STMT_CREATE_INDEX:
	{
		// handled separately
		abort();
		break;
	}
	case STMT_DROP_INDEX:
	{
		auto arg = ast_index_drop_of(stmt->ast);
		offset = table_op_index_drop(data, user, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_INDEX:
	{
		auto arg = ast_index_alter_of(stmt->ast);
		offset = table_op_index_rename(data, user, &arg->table_name, &arg->name,
		                               &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// branch
	case STMT_CREATE_BRANCH:
	{
		auto arg = ast_branch_create_of(stmt->ast);
		offset = table_op_branch_create(data, user, &arg->table_name, arg->config);
		flags  = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_BRANCH:
	{
		auto arg = ast_branch_drop_of(stmt->ast);
		offset = table_op_branch_drop(data, user, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_BRANCH:
	{
		auto arg = ast_branch_alter_of(stmt->ast);
		offset = table_op_branch_rename(data, user, &arg->table_name, &arg->name,
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
		offset = udf_op_drop(data, user, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_FUNCTION:
	{
		auto arg = ast_function_alter_of(stmt->ast);
		offset = udf_op_rename(data, user, &arg->name, user, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// synonym
	case STMT_CREATE_SYNONYM:
	{
		auto arg = ast_synonym_create_of(stmt->ast);
		offset = synonym_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_SYNONYM:
	{
		auto arg = ast_synonym_drop_of(stmt->ast);
		offset = synonym_op_drop(data, user, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_SYNONYM:
	{
		auto arg = ast_synonym_alter_of(stmt->ast);
		offset = synonym_op_rename(data, user, &arg->name, user, &arg->name_new);
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

	// show(section, name, on, extended)

	// section
	auto r = emit_string(self, &arg->section, false);
	op1(self, CPUSH, r);
	runpin(self, r);

	// name
	r = emit_string(self, &arg->name, false);
	op1(self, CPUSH, r);
	runpin(self, r);

	// on
	r = emit_string(self, &arg->on, false);
	op1(self, CPUSH, r);
	runpin(self, r);

	// extended
	r = op2pin(self, CBOOL, TYPE_BOOL, arg->extended);
	op1(self, CPUSH, r);
	runpin(self, r);

	r = op4pin(self, CCALL, fn->type, (intptr_t)fn, 4, -1);
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
	program->utility = true;
	auto lock_catalog = LOCK_EXCLUSIVE;
	auto lock_ddl     = LOCK_EXCLUSIVE;

	int r = -1;
	switch (stmt->id) {
	// system
	case STMT_SHOW:
	{
		r = emit_show(self);

		// lock
		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_NONE;
		break;
	}
	case STMT_ALTER_SYSTEM:
	{
		auto arg = ast_system_alter_of(stmt->ast);
		unused(arg);
		op0(self, CCREATE_SECRET);

		lock_catalog = LOCK_NONE;
		lock_ddl     = LOCK_NONE;
		break;
	}
	case STMT_CHECKPOINT:
	{
		auto arg = ast_checkpoint_of(stmt->ast);
		unused(arg);
		op0(self, CCHECKPOINT);

		// lock
		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_EXCLUSIVE;
		break;
	}

	// token
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
		r = op2pin(self, CCREATE_TOKEN, TYPE_JSON, offset);

		// lock
		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_EXCLUSIVE;
		break;
	}

	// replica
	case STMT_CREATE_REPLICA:
	{
		auto arg = ast_replica_create_of(stmt->ast);
		auto offset = buf_size(data);
		replica_config_write(arg->config, data, 0);
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

	// service
	case STMT_ALTER_PARTITION:
	{
		auto arg = ast_part_alter_of(stmt->ast);
		auto table = catalog_find_table(&share()->db->catalog,
		                                self->parser.user,
		                                &arg->table->string, true);
		if (arg->type == PARTITION_ALTER_REFRESH)
		{
			op3(self, CDDL_REFRESH, (intptr_t)table, arg->id, -1);
		} else
		if (arg->type == PARTITION_ALTER_MOVE)
		{
			auto offset = code_data_add_string(self->code_data, &arg->storage);
			op3(self, CDDL_REFRESH, (intptr_t)table, arg->id, offset);
		}

		// lock
		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_NONE;
		break;
	}

	// create index
	case STMT_CREATE_INDEX:
	{
		auto arg    = ast_index_create_of(stmt->ast);
		auto user     = self->parser.user;
		auto offset = table_op_index_create(data, user, &arg->table_name, arg->config);
		auto flags  = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		op2(self, CDDL_CREATE_INDEX, offset, flags);

		// start without locks and require manual locking control
		// during execution
		lock_catalog = LOCK_NONE;
		lock_ddl     = LOCK_EXCLUSIVE;
		break;
	}

	// locking
	case STMT_CREATE_LOCK:
	{
		auto arg = ast_lock_create_of(stmt->ast);
		auto name      = code_data_add_string(self->code_data, &arg->name);
		auto name_rel  = code_data_add_string(self->code_data, &arg->name_rel);
		auto name_lock = code_data_add_string(self->code_data, &arg->name_lock);
		op4(self, CLOCK, name, name_rel, name_lock, arg->if_not_exists);

		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_EXCLUSIVE;
		break;
	}
	case STMT_DROP_LOCK:
	{
		auto arg = ast_lock_drop_of(stmt->ast);
		auto name = code_data_add_string(self->code_data, &arg->name);
		op2(self, CUNLOCK, name, arg->if_exists);

		lock_catalog = LOCK_SHARED;
		lock_ddl     = LOCK_EXCLUSIVE;
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

	// set catalog lock
	program->lock_catalog = lock_catalog;
	program->lock_ddl     = lock_ddl;
}
