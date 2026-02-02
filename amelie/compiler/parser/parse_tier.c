
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

void
parse_tier_create(Stmt* self)
{
	// CREATE TIER [IF NOT EXISTS] name ON table_name [(options)]
	// [USING storage, ...]
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

	// create tier
	auto tier = tier_allocate();
	stmt->tier = tier;
	tier_set_name(tier, &name->string);

	// generate id
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
	tier_set_id(tier, &id);

	// [(options)]
	if (stmt_if(self, '(') && !stmt_if(self, ')'))
	{
		for (;;)
		{
			// name value
			auto name = stmt_expect(self, KNAME);

			if (str_is(&name->string, "id", 2))
			{
				auto value = stmt_expect(self, KSTRING);
				uuid_init(&id);
				if (uuid_set_nothrow(&id, &value->string) == -1)
					stmt_error(self, value, "failed to parse uuid");
				tier_set_id(stmt->tier, &id);
			} else
			if (str_is(&name->string, "compression", 11))
			{
				auto value = stmt_expect(self, KSTRING);
				tier_set_compression(stmt->tier, &value->string);
			} else
			if (str_is(&name->string, "compression_level", 17))
			{
				auto value = stmt_expect(self, KINT);
				tier_set_compression_level(stmt->tier, value->integer);
			} else
			if (str_is(&name->string, "encryption", 10))
			{
				auto value = stmt_expect(self, KSTRING);
				tier_set_encryption(stmt->tier, &value->string);
			} else
			if (str_is(&name->string, "encryption_key", 14))
			{
				auto value = stmt_expect(self, KSTRING);
				tier_set_encryption_key(stmt->tier, &value->string);
			} else
			if (str_is(&name->string, "region_size", 11))
			{
				auto value = stmt_expect(self, KINT);
				tier_set_region_size(stmt->tier, value->integer);
			} else
			if (str_is(&name->string, "object_size", 11))
			{
				auto value = stmt_expect(self, KINT);
				tier_set_object_size(stmt->tier, value->integer);
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
	}

	// [USING storage, ...]
	if (stmt_if(self, KUSING))
	{
		for (;;)
		{
			auto name = stmt_expect(self, KNAME);
			auto storage = tier_storage_find(tier, &name->string);
			if (storage)
				stmt_error(self, name, "storage used twice");
			storage = tier_storage_allocate();
			tier_storage_set_name(storage, &name->string);
			tier_storage_add(tier, storage);

			// ,
			if (stmt_if(self, ','))
				continue;

			break;
		}
	} else
	{
		// add main storage by default
		Str main;
		str_set(&main, "main", 4);
		auto storage = tier_storage_allocate();
		tier_storage_set_name(storage, &main);
		tier_storage_add(tier, storage);
	}
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
	// ALTER TIER [IF EXISTS] name ON table_name ADD STORAGE [IF EXISTS] name
	// ALTER TIER [IF EXISTS] name ON table_name DROP STORAGE [IF EXISTS] name
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

	// RENAME | ADD | DROP
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

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_storage = name->string;
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
	{
		stmt_error(self, NULL, "RENAME | ADD | DROP expected");
	}
}
