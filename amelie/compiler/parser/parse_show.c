
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

static inline int
parse_show_type(Str* name)
{
	if (str_is(name, "users", 5))
		return SHOW_USERS;

	if (str_is(name, "user", 4))
		return SHOW_USER;

	if (str_is(name, "replicas", 8))
		return SHOW_REPLICAS;

	if (str_is(name, "replica", 7))
		return SHOW_REPLICA;

	if (str_is(name, "repl", 4) ||
	    str_is(name, "replication", 11))
		return SHOW_REPL;

	if (str_is(name, "wal", 3))
		return SHOW_WAL;

	if (str_is(name, "metrics", 7))
		return SHOW_METRICS;

	if (str_is(name, "schemas", 7))
		return SHOW_SCHEMAS;

	if (str_is(name, "schema", 6))
		return SHOW_SCHEMA;

	if (str_is(name, "tables", 6))
		return SHOW_TABLES;

	if (str_is(name, "table", 5))
		return SHOW_TABLE;

	if (str_is(name, "procedures", 10))
		return SHOW_PROCEDURES;

	if (str_is(name, "procedure", 9))
		return SHOW_PROCEDURE;

	if (str_is(name, "state", 5))
		return SHOW_STATE;

	if (str_is(name, "config", 6) ||
	    str_is(name, "all", 3))
		return SHOW_CONFIG_ALL;

	return SHOW_CONFIG;
}

void
parse_show(Stmt* self)
{
	// SHOW <SECTION> [name] [IN|FROM schema] [extended] [FORMAT type]
	auto stmt = ast_show_allocate();
	self->ast = &stmt->ast;

	// section | option name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME)
		stmt_error(self, name, "name expected");
	stmt->section = name->string;

	stmt->type = parse_show_type(&name->string);
	switch (stmt->type) {
	case SHOW_USER:
	case SHOW_REPLICA:
	case SHOW_SCHEMA:
		name = stmt_next_shadow(self);
		if (name->id != KNAME && name->id != KSTRING)
			stmt_error(self, name, "name expected");
		stmt->name_ast = name;
		stmt->name = name->string;
		break;
	case SHOW_TABLES:
	case SHOW_TABLE:
	{
		// [table name]
		if (stmt->type == SHOW_TABLE)
		{
			name = stmt_next(self);
			if (name->id == KNAME) {
				stmt->name_ast = name;
				stmt->name = name->string;
				str_set(&stmt->schema, "public", 6);
			} else
			if (name->id == KNAME_COMPOUND)
			{
				stmt->name_ast = name;
				stmt->name = name->string;
				str_split(&stmt->name, &stmt->schema, '.');
				str_advance(&stmt->name, str_size(&stmt->schema) + 1);
			} else {
				stmt_error(self, name, "table name expected");
			}
		}

		// [IN | FROM schema]
		if (stmt_if(self, KIN) || stmt_if(self, KFROM))
		{
			auto schema = stmt_expect(self, KNAME);
			stmt->schema = schema->string;
		}
		break;
	}
	case SHOW_PROCEDURES:
	case SHOW_PROCEDURE:
	{
		// [procedure name]
		if (stmt->type == SHOW_PROCEDURE)
		{
			name = stmt_next(self);
			if (name->id == KNAME) {
				stmt->name_ast = name;
				stmt->name = name->string;
				str_set(&stmt->schema, "public", 6);
			} else
			if (name->id == KNAME_COMPOUND)
			{
				stmt->name_ast = name;
				stmt->name = name->string;
				str_split(&stmt->name, &stmt->schema, '.');
				str_advance(&stmt->name, str_size(&stmt->schema) + 1);
			} else {
				stmt_error(self, name, "procedure name expected");
			}
		}

		// [IN | FROM schema]
		if (stmt_if(self, KIN) || stmt_if(self, KFROM))
		{
			auto schema = stmt_expect(self, KNAME);
			stmt->schema = schema->string;
		}
		break;
	}
	case SHOW_CONFIG:
		stmt->name_ast = name;
		stmt->name = name->string;
		break;
	default:
		break;
	}

	// [EXTENDED]
	stmt->extended = stmt_if(self, KEXTENDED) != NULL;

	// [FORMAT type]
	if (stmt_if(self, KFORMAT))
	{
		auto type = stmt_expect(self, KSTRING);
		stmt->format = type->string;
	}
}
