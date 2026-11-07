
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
		offset = rel_op_rename(data, REL_TABLE, &arg->name, &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_DESCRIPTION:
	{
		offset = rel_op_describe(data, REL_TABLE, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case TABLE_ALTER_COLUMN_RENAME:
	{
		offset = table_op_column_rename(data, &arg->name, &arg->column_name,
		                                &arg->name_new);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_ADD:
	{
		offset = table_op_column_add(data, &arg->name, arg->column);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_not_exists)
			flags |= DDL_IF_COLUMN_NOT_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_DROP:
	{
		offset = table_op_column_drop(data, &arg->name, &arg->column_name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_COLUMN_SET_DEFAULT:
	case TABLE_ALTER_COLUMN_UNSET_DEFAULT:
	{
		offset = table_op_column_set(data, DDL_TABLE_COLUMN_SET_DEFAULT,
		                             &arg->name,
		                             &arg->column_name,
		                             &arg->value);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		if (arg->if_column_exists)
			flags |= DDL_IF_COLUMN_EXISTS;
		break;
	}
	case TABLE_ALTER_RETENTION:
	{
		offset = table_op_retention(data, &arg->name, arg->retention);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
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
	auto stmt      = compiler_stmt(self);
	auto data      = &self->code_data->data;
	auto offset    = 0;
	auto flags     = 0;
	int  databases = false;
	switch (stmt->id) {
	// database
	case STMT_CREATE_DATABASE:
	{
		auto arg = ast_database_create_of(stmt->ast);
		offset = database_op_create(data, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		databases = true;
		break;
	}
	case STMT_DROP_DATABASE:
	{
		auto arg = ast_database_drop_of(stmt->ast);
		offset = database_op_drop(data, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		databases = true;
		break;
	}
	case STMT_ALTER_DATABASE:
	{
		auto arg = ast_database_alter_of(stmt->ast);
		if (arg->type == DATABASE_ALTER_RENAME)
			offset = database_op_rename(data, &arg->name, &arg->name_new);
		else
		if (arg->type == DATABASE_ALTER_REVOKE_TOKEN)
			offset = database_op_revoke_token(data, &arg->name, &arg->revoked_at);
		else
		if (arg->type == DATABASE_ALTER_DESCRIPTION)
			offset = database_op_describe(data, &arg->name, &arg->description);
		else
		if (arg->type == DATABASE_ALTER_LIMIT_SET)
			offset = database_op_limit_set(data, &arg->name, &arg->limits);
		else
		if (arg->type == DATABASE_ALTER_LIMIT_UNSET)
			offset = database_op_limit_unset(data, &arg->name, arg->limits_mask);
		else
		if (arg->type == DATABASE_ALTER_API_ADD)
			offset = database_op_api_create(data, &arg->name, arg->api);
		else
		if (arg->type == DATABASE_ALTER_API_DROP)
			offset = database_op_api_drop(data, &arg->name, &arg->api_uri);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		databases = true;
		break;
	}
	case STMT_CREATE_API:
	{
		auto arg = ast_api_create_of(stmt->ast);
		offset = database_op_api_create(data, &self->parser.local->database, arg->config);
		databases = true;
		break;
	}
	case STMT_DROP_API:
	{
		auto arg = ast_api_drop_of(stmt->ast);
		offset = database_op_api_drop(data, &self->parser.local->database, &arg->uri);
		databases = true;
		break;
	}

#if 0
	// grant
	case STMT_GRANT:
	case STMT_REVOKE:
	{
		auto arg = ast_grant_of(stmt->ast);
		offset = rel_op_grant(data, &arg->rel, &arg->to, arg->grant, arg->perms);
		break;
	}
#endif

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
		offset = rel_op_drop(data, REL_TABLE, &arg->name, arg->cascade);
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
		offset = table_op_truncate(data, &arg->name);
		break;
	}

	// create index
	case STMT_CREATE_INDEX:
	{
		auto arg    = ast_index_create_of(stmt->ast);
		offset = table_op_index_create(data, &arg->table_name, arg->config);
		flags = arg->if_not_exists ? DDL_IF_NOT_EXISTS : 0;
		break;
	}
	case STMT_DROP_INDEX:
	{
		auto arg = ast_index_drop_of(stmt->ast);
		offset = table_op_index_drop(data, &arg->table_name, &arg->name);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_INDEX:
	{
		auto arg = ast_index_alter_of(stmt->ast);
		if (arg->type == INDEX_ALTER_RENAME)
			offset = table_op_index_rename(data, &arg->table_name,
			                               &arg->name, &arg->name_new);
		else
		if (arg->type == INDEX_ALTER_PRIMARY)
			offset = table_op_index_primary(data, &arg->table_name, &arg->name, arg->pk);
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
		offset = rel_op_drop(data, REL_UDF, &arg->name, arg->cascade);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}
	case STMT_ALTER_FUNCTION:
	{
		auto arg = ast_function_alter_of(stmt->ast);
		if (arg->type == FUNCTION_ALTER_RENAME)
			offset = rel_op_rename(data, REL_UDF, &arg->name, &arg->name_new);
		else
		if (arg->type == FUNCTION_ALTER_DESCRIPTION)
			offset = rel_op_describe(data, REL_UDF, &arg->name, &arg->description);
		flags = arg->if_exists ? DDL_IF_EXISTS : 0;
		break;
	}

	default:
		abort();
		break;
	}
	op3(self, CDDL, offset, flags, databases);
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
	program->utility = true;

	int r = -1;
	switch (stmt->id) {
	// system
	case STMT_SHOW:
	{
		r = emit_show(self);
		break;
	}
	case STMT_ALTER_SYSTEM:
	{
		auto arg = ast_system_alter_of(stmt->ast);
		unused(arg);

		if (arg->type == SYSTEM_ALTER_SET_SECRET)
		{
			auto offset = buf_size(data);
			encode_str(data, &arg->secret);
			op1(self, CSYSTEM_SET_SECRET, offset);
		}
		break;
	}
	case STMT_CHECKPOINT:
	{
		auto arg = ast_checkpoint_of(stmt->ast);
		unused(arg);
		op0(self, CCHECKPOINT);
		break;
	}

	case STMT_BACKUP:
	{
		auto arg = ast_backup_of(stmt->ast);

		// set id
		auto offset = code_data_offset(self->code_data);
		auto uuid   = (Uuid*)buf_emplace(&self->code_data->data, sizeof(Uuid));
		*uuid = arg->id;
		op1(self, CBACKUP, offset);
		break;
	}

	// token
	case STMT_CREATE_TOKEN:
	{
		auto arg = ast_token_create_of(stmt->ast);
		auto offset = buf_size(data);
		encode_str(data, &arg->database->string);
		Str str;
		str_set_cstr(&str, "1 day");
		if (arg->expire)
			str = arg->expire->string;
		encode_str(data, &str);

		r = op2pin(self, CCREATE_TOKEN, TYPE_JSON, offset);
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
