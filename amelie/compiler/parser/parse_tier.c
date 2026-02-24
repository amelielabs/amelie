
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

static int
parse_tier_options(Stmt* self, TierConfig* config)
{
	int mask = 0;
	for (;;)
	{
		// name value
		auto name = stmt_expect(self, KNAME);

		if (str_is(&name->string, "region_size", 11))
		{
			auto value = stmt_expect(self, KINT);
			tier_config_set_region_size(config, value->integer);
			mask |= TIER_REGION_SIZE;
		} else
		if (str_is(&name->string, "object_size", 11))
		{
			auto value = stmt_expect(self, KINT);
			tier_config_set_object_size(config, value->integer);
			mask |= TIER_OBJECT_SIZE;
		} else
		{
			stmt_error(self, name, "unknown option");
		}

		// )
		if (stmt_if(self, ')'))
			break;

		// ,
		stmt_expect(self, ',');
	}
	return mask;
}

void
parse_tier_create(Stmt* self)
{
	// CREATE TIER [IF NOT EXISTS] name ON table_name [(options)]
	// [ON STORAGE]
	auto stmt = ast_tier_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// ON
	stmt_expect(self, KON);

	// table_name
	auto target = stmt_expect(self, KNAME);
	stmt->table_name = target->string;

	// allocate tier
	auto config = tier_config_allocate();
	stmt->config = config;
	tier_config_set_name(config, &name->string);

	// [(options)]
	if (stmt_if(self, '(') && !stmt_if(self, ')'))
		parse_tier_options(self, config);

	// [ON STORAGE]
	parse_volumes(self, &config->volumes);
}

void
parse_tier_drop(Stmt* self)
{
	// DROP TIER [IF EXISTS] name ON table_name
	auto stmt = ast_tier_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;
}

void
parse_tier_alter(Stmt* self)
{
	// ALTER TIER [IF EXISTS] name ON table_name RENAME TO name
	// ALTER TIER [IF EXISTS] name ON table_name ADD STORAGE [IF NOT EXISTS] name [(options])
	// ALTER TIER [IF EXISTS] name ON table_name DROP STORAGE [IF EXISTS] name
	// ALTER TIER [IF EXISTS] name ON table_name PAUSE STORAGE [IF EXISTS] name
	// ALTER TIER [IF EXISTS] name ON table_name RESUME STORAGE [IF EXISTS] name
	// ALTER TIER [IF EXISTS] name ON table_name SET (options)
	auto stmt = ast_tier_alter_allocate();
	self->ast = &stmt->ast;

	// [IF EXISTS]
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;

	// RENAME | ADD | DROP | PAUSE | RESUME | SET
	if (stmt_if(self, KRENAME))
	{
		stmt->type = TIER_ALTER_RENAME;

		// TO
		stmt_expect(self, KTO);

		// name
		name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
	} else
	if (stmt_if(self, KADD))
	{
		stmt->type = TIER_ALTER_STORAGE_ADD;

		// STORAGE
		stmt_expect(self, KSTORAGE);

		// [IF NOT EXISTS]
		stmt->if_not_exists_storage = parse_if_not_exists(self);

		// name ([options])
		stmt->volume = parse_volume(self);

	} else
	if (stmt_if(self, KDROP))
	{
		stmt->type = TIER_ALTER_STORAGE_DROP;

		// STORAGE
		stmt_expect(self, KSTORAGE);

		// [IF EXISTS]
		stmt->if_exists_storage = parse_if_exists(self);

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_storage = name->string;
	} else
	if (stmt_if(self, KPAUSE))
	{
		stmt->type = TIER_ALTER_STORAGE_PAUSE;

		// STORAGE
		stmt_expect(self, KSTORAGE);

		// [IF EXISTS]
		stmt->if_exists_storage = parse_if_exists(self);

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_storage = name->string;
	} else
	if (stmt_if(self, KRESUME))
	{
		stmt->type = TIER_ALTER_STORAGE_RESUME;

		// STORAGE
		stmt_expect(self, KSTORAGE);

		// [IF EXISTS]
		stmt->if_exists_storage = parse_if_exists(self);

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_storage = name->string;
	} else
	if (stmt_if(self, KSET))
	{
		stmt->type = TIER_ALTER_SET;

		// allocate tier
		stmt->set = tier_config_allocate();
		tier_config_set_name(stmt->set, &name->string);

		// (options)
		stmt_expect(self, '(');
		stmt->set_mask = parse_tier_options(self, stmt->set);
	} else
	{
		stmt_error(self, NULL, "RENAME | ADD | DROP | PAUSE | RESUME | SET expected");
	}
}
