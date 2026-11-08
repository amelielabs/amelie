
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

static void
parse_database_limits(Stmt* self, Limits* limits)
{
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "limit name expected");

		// =
		stmt_expect(self, '=');

		// value
		auto value = stmt_expect(self, KINT);
		auto id = limits_find(&name->string);
		if (id == -1)
			stmt_error(self, name, "failed to find the limit");
		limits_set(limits, id, value->integer);

		// ',
		if (stmt_if(self, ','))
			continue;

		break;
	}
}

static uint64_t
parse_database_limits_mask(Stmt* self)
{
	uint64_t mask = 0;
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "limit name expected");
		auto id = limits_find(&name->string);
		if (id == -1)
			stmt_error(self, name, "failed to find the limit");
		mask |= (1 << id);

		// ',
		if (stmt_if(self, ','))
			continue;

		break;
	}
	return mask;
}

void
parse_database_create(Stmt* self)
{
	// CREATE DATABASE [IF NOT EXISTS] name
	// [DESCRIPTION]
	// [LIMIT]
	// [API]
	auto stmt = ast_database_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create database config
	auto config = db_config_allocate();
	stmt->config = config;

	// name
	auto name = stmt_expect(self, KNAME);

	/*db_config_set_parent(config, &database);*/
	db_config_set_name(config, &name->string);

	// set timestamp
	char ts[64];
	auto time = time_us();
	auto size = timestamp_get(time, runtime()->timezone, ts, sizeof(ts));
	Str created_at;
	str_set(&created_at, ts, size);
	db_config_set_created_at(config, &created_at);

	// set options
	for (;;)
	{
		// name value
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
		{
			stmt_push(self, name);
			break;
		}

		// DESCRIPTION string
		if (str_is_case(&name->string, "description", 11))
		{
			auto text = stmt_expect(self, KSTRING);
			db_config_set_description(config, &text->string);
			continue;
		}

		// LIMIT name = value, ...
		if (str_is_case(&name->string, "limit", 5))
		{
			parse_database_limits(self, &config->limits);
			continue;
		}

		// API uri ON target
		if (str_is_case(&name->string, "api", 3))
		{
			parse_api_create_inline(self, &config->apis);
			continue;
		}

		// CREATED string
		if (str_is_case(&name->string, "created", 7))
		{
			auto value = stmt_expect(self, KSTRING);

			// validate timestamp
			Timestamp ts;
			timestamp_init(&ts);
			if (unlikely(error_catch( timestamp_set(&ts, &value->string) )))
				stmt_error(self, value, "invalid timestamp value");
			db_config_set_created_at(config, &value->string);
			continue;
		}

		// REVOKED string
		if (str_is_case(&name->string, "revoked", 7))
		{
			auto value = stmt_expect(self, KSTRING);

			// validate timestamp
			Timestamp ts;
			timestamp_init(&ts);
			if (unlikely(error_catch( timestamp_set(&ts, &value->string) )))
				stmt_error(self, value, "invalid timestamp value");
			db_config_set_revoked_at(config, &value->string);
			continue;
		}

		stmt_error(self, name, "unrecognized option");
	}
}

void
parse_database_drop(Stmt* self)
{
	// DROP DATABASE [IF EXISTS] name
	auto stmt = ast_database_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;
}

void
parse_database_alter(Stmt* self)
{
	// ALTER DATABASE [IF EXISTS] name RENAME name
	// ALTER DATABASE [IF EXISTS] name REVOKE TOKEN
	// ALTER DATABASE [IF EXISTS] name DESCRIPTION value
	// ALTER DATABASE [IF EXISTS] name SET LIMIT name = value, ...
	// ALTER DATABASE [IF EXISTS] name UNSET LIMIT name, ...
	// ALTER DATABASE [IF EXISTS] name ADD API uri ON target
	// ALTER DATABASE [IF EXISTS] name DROP API uri
	auto stmt = ast_database_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME | REVOKE | DESCRIPTION | SET | UNSET | ADD | DROP
	if (stmt_if(self, KRENAME))
	{
		// RENAME
		stmt->type = DATABASE_ALTER_RENAME;

		// TO
		stmt_expect(self, KTO);

		// name
		name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
	} else
	if (stmt_if(self, KREVOKE))
	{
		// TOKEN
		stmt_expect(self, KTOKEN);

		// REVOKE
		stmt->type = DATABASE_ALTER_REVOKE_TOKEN;

		// set timestamp
		auto ts = palloc(64);
		auto time = time_us();
		auto size = timestamp_get(time, runtime()->timezone, ts, 64);
		str_set(&stmt->revoked_at, ts, size);
	} else
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = DATABASE_ALTER_DESCRIPTION;
		stmt->description = text->string;
	} else
	if (stmt_if(self, KSET))
	{
		// SET LIMIT name = value, ...
		stmt_expect(self, KLIMIT);
		stmt->type = DATABASE_ALTER_LIMIT_SET;
		parse_database_limits(self, &stmt->limits);

	} else
	if (stmt_if(self, KUNSET))
	{
		// UNSET LIMIT name, ...
		stmt_expect(self, KLIMIT);
		stmt->type = DATABASE_ALTER_LIMIT_UNSET;
		stmt->limits_mask = parse_database_limits_mask(self);
	} else
	if (stmt_if(self, KADD))
	{
		stmt_expect(self, KAPI);
		stmt->type = DATABASE_ALTER_API_ADD;

		Apis apis;
		apis_init(&apis);
		errdefer(apis_free, &apis);
		parse_api_create_inline(self, &apis);
		stmt->api = apis_first(&apis);
	} else
	if (stmt_if(self, KDROP))
	{
		stmt_expect(self, KAPI);
		stmt->type = DATABASE_ALTER_API_DROP;
		auto uri = stmt_expect(self, KSTRING);
		stmt->api_uri = uri->string;
	} else
	{
		stmt_error(self, NULL, "operation name expected");
	}
}
