
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
#include <amelie_plan.h>
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
		offset = rel_op_rename(data, REL_TABLE, &arg->user, &arg->name, &arg->user, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_DESCRIPTION:
	{
		offset = rel_op_describe(data, REL_TABLE, &arg->user, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_COLUMN_RENAME:
	{
		offset = table_op_column_rename(data, &arg->user, &arg->name, &arg->column_name,
		                                &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_ADD:
	{
		offset = table_op_column_add(data, &arg->user, &arg->name, arg->column);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_not_exists)
			flags |= DDL_IF_COLUMN_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_DROP:
	{
		offset = table_op_column_drop(data, &arg->user, &arg->name, &arg->column_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_DEFAULT,
		                             &arg->user, &arg->name,
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
	// grant
	case STMT_GRANT:
	case STMT_REVOKE:
	{
		auto arg = ast_grant_of(stmt->ast);
		offset = rel_op_grant(data, &arg->rel_user, &arg->rel, &arg->to,
		                      arg->grant, arg->perms);
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
		offset = user_op_drop(data, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_USER:
	{
		auto arg = ast_user_alter_of(stmt->ast);
		if (arg->type == USER_ALTER_RENAME)
			offset = user_op_rename(data, &arg->name, &arg->name_new);
		else
		if (arg->type == USER_ALTER_REVOKE_TOKEN)
			offset = user_op_revoke_token(data, &arg->name, &arg->revoked_at);
		else
		if (arg->type == USER_ALTER_DESCRIPTION)
			offset = user_op_describe(data, &arg->name, &arg->description);
		else
		if (arg->type == USER_ALTER_LIMIT_SET)
			offset = user_op_limit_set(data, &arg->name, &arg->limits);
		else
		if (arg->type == USER_ALTER_LIMIT_UNSET)
			offset = user_op_limit_unset(data, &arg->name, arg->limits_mask);
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
		offset = rel_op_drop(data, REL_TABLE, &arg->user, &arg->name, arg->cascade);
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
		offset = table_op_truncate(data, &arg->user, &arg->name);
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
		offset = table_op_index_drop(data, &arg->table_user, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_INDEX:
	{
		auto arg = ast_index_alter_of(stmt->ast);
		offset = table_op_index_rename(data, &arg->table_user, &arg->table_name, &arg->name,
		                               &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// clone
	case STMT_CREATE_CLONE:
	{
		auto arg = ast_clone_create_of(stmt->ast);
		offset = clone_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_CLONE:
	{
		auto arg = ast_clone_drop_of(stmt->ast);
		offset = rel_op_drop(data, REL_CLONE, &arg->user, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_CLONE:
	{
		auto arg = ast_clone_alter_of(stmt->ast);
		if (arg->type == CLONE_ALTER_RENAME)
			offset = rel_op_rename(data, REL_CLONE, &arg->user, &arg->name, &arg->user, &arg->name_new);
		else
		if (arg->type == CLONE_ALTER_DESCRIPTION)
			offset = rel_op_describe(data, REL_CLONE, &arg->user, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// function
	case STMT_CREATE_FUNCTION:
	{
		auto arg = ast_function_create_of(stmt->ast);
		offset = udf_op_create(data, arg->config, arg->or_replace);
		flags = 0;
		break;
	}
	case STMT_DROP_FUNCTION:
	{
		auto arg = ast_function_drop_of(stmt->ast);
		offset = rel_op_drop(data, REL_UDF, &arg->user, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_FUNCTION:
	{
		auto arg = ast_function_alter_of(stmt->ast);
		if (arg->type == FUNCTION_ALTER_RENAME)
			offset = rel_op_rename(data, REL_UDF, &arg->user, &arg->name, &arg->user, &arg->name_new);
		else
		if (arg->type == FUNCTION_ALTER_DESCRIPTION)
			offset = rel_op_describe(data, REL_UDF, &arg->user, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// topic
	case STMT_CREATE_TOPIC:
	{
		auto arg = ast_topic_create_of(stmt->ast);
		offset = topic_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_TOPIC:
	{
		auto arg = ast_topic_drop_of(stmt->ast);
		offset = rel_op_drop(data, REL_TOPIC, &arg->user, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_TOPIC:
	{
		auto arg = ast_topic_alter_of(stmt->ast);
		if (arg->type == TOPIC_ALTER_RENAME)
			offset = rel_op_rename(data, REL_TOPIC, &arg->user, &arg->name, &arg->user, &arg->name_new);
		else
		if (arg->type == TOPIC_ALTER_DESCRIPTION)
			offset = rel_op_describe(data, REL_TOPIC, &arg->user, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	// subscription
	case STMT_CREATE_SUBSCRIPTION:
	{
		auto arg = ast_sub_create_of(stmt->ast);
		offset = sub_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_SUBSCRIPTION:
	{
		auto arg = ast_sub_drop_of(stmt->ast);
		offset = rel_op_drop(data, REL_SUBSCRIPTION, &arg->user, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_SUBSCRIPTION:
	{
		auto arg = ast_sub_alter_of(stmt->ast);
		if (arg->type == SUBSCRIPTION_ALTER_RENAME)
			offset = rel_op_rename(data, REL_SUBSCRIPTION, &arg->user, &arg->name, &arg->user, &arg->name_new);
		else
		if (arg->type == SUBSCRIPTION_ALTER_DESCRIPTION)
			offset = rel_op_describe(data, REL_SUBSCRIPTION, &arg->user, &arg->name, &arg->description);
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

	// find show() functions
	Str name;
	str_set(&name, "show", 4);
	auto fn = functions_find(share()->functions, &name);
	assert(fn);

	// show(section, name, on, flags)

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

	// flags
	int flags = FFROM;
	if (arg->all)
		flags |= FALL;
	if (arg->create)
		flags |= FCREATE;
	if (! arg->verbose)
		flags |= FMINIMAL;
	r = op2pin(self, CINT, TYPE_INT, flags);
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
		if (arg->type == SYSTEM_ALTER_SECRET_ROTATE)
		{
			op0(self, CCREATE_SECRET);
		} else
		if (arg->type == SYSTEM_ALTER_SET_CDC ||
		    arg->type == SYSTEM_ALTER_UNSET_CDC)
		{
			op1(self, CCDC_LIMIT, arg->cdc_limit);
		}
		lock_catalog = LOCK_SHARED;
		break;
	}
	case STMT_CHECKPOINT:
	{
		auto arg = ast_checkpoint_of(stmt->ast);
		unused(arg);
		op0(self, CCHECKPOINT);

		// lock
		lock_catalog = LOCK_NONE;
		lock_ddl     = LOCK_NONE;
		break;
	}

	// token
	case STMT_CREATE_TOKEN:
	{
		auto arg = ast_token_create_of(stmt->ast);
		auto offset = buf_size(data);
		encode_str(data, &arg->user->string);
		Str str;
		str_set_cstr(&str, "1 day");
		if (arg->expire)
			str = arg->expire->string;
		encode_str(data, &str);

		r = op2pin(self, CCREATE_TOKEN, TYPE_JSON, offset);

		// lock
		lock_catalog = LOCK_SHARED;
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
		encode_str(data, &arg->id->string);
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
	case STMT_FOLLOW:
	{
		auto arg = ast_repl_follow_of(stmt->ast);
		auto offset = code_data_add_string(self->code_data, &arg->id->string);
		op1(self, CREPL_FOLLOW, offset);
		break;
	}
	case STMT_UNFOLLOW:
	{
		op0(self, CREPL_UNFOLLOW);
		break;
	}

	// create index
	case STMT_CREATE_INDEX:
	{
		auto arg    = ast_index_create_of(stmt->ast);
		auto offset = table_op_index_create(data, &arg->table_user, &arg->table_name, arg->config);
		auto flags  = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		op2(self, CDDL_CREATE_INDEX, offset, flags);

		// start without locks and require manual locking control
		// during execution
		lock_catalog = LOCK_NONE;
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
		break;
	}
	case STMT_DROP_LOCK:
	{
		auto arg = ast_lock_drop_of(stmt->ast);
		auto name = code_data_add_string(self->code_data, &arg->name);
		op2(self, CUNLOCK, name, arg->if_exists);

		lock_catalog = LOCK_SHARED;
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
