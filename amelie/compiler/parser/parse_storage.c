
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

void
parse_storage_create(Stmt* self)
{
	// CREATE STORAGE [IF NOT EXISTS] name [(options)]
	auto stmt = ast_storage_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create storage config
	stmt->config = storage_config_allocate();
	Str compression;
	str_set(&compression, "zstd", 4);
	storage_config_set_compression(stmt->config, &compression);
	storage_config_set_compression_level(stmt->config, 0);

	// name
	auto name = stmt_expect(self, KNAME);
	storage_config_set_name(stmt->config, &name->string);

	// ()
	if (!stmt_if(self, '(') || stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name value
		auto name = stmt_expect(self, KNAME);
		if (str_is(&name->string, "path", 4))
		{
			// path string
			auto path = stmt_expect(self, KSTRING);
			storage_config_set_path(stmt->config, &path->string);
		} else
		if (str_is(&name->string, "compression", 11))
		{
			auto value = stmt_expect(self, KSTRING);
			storage_config_set_compression(stmt->config, &value->string);
		} else
		if (str_is(&name->string, "compression_level", 17))
		{
			auto value = stmt_expect(self, KINT);
			storage_config_set_compression_level(stmt->config, value->integer);
		} else {
			stmt_error(self, name, "unknown option");
		}

		// )
		if (stmt_if(self, ')'))
			break;

		// ,
		stmt_expect(self, ',');
	}
}

void
parse_storage_drop(Stmt* self)
{
	// DROP STORAGE [IF EXISTS] name
	auto stmt = ast_storage_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);
}

void
parse_storage_alter(Stmt* self)
{
	// ALTER STORAGE [IF EXISTS] name RENAME name
	auto stmt = ast_storage_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	stmt->name_new = stmt_expect(self, KNAME);
}

static void
parse_volume_options(Stmt* self, Volume* volume)
{
	for (;;)
	{
		// name value
		auto name = stmt_expect(self, KNAME);

		if (str_is(&name->string, "id", 2))
		{
			auto value = stmt_expect(self, KSTRING);
			Uuid id;
			uuid_init(&id);
			if (uuid_set_nothrow(&id, &value->string) == -1)
				stmt_error(self, value, "failed to parse uuid");
			volume_set_id(volume, &id);
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

Volume*
parse_volume(Stmt* self)
{
	auto volume = volume_allocate();
	errdefer(volume_free, volume);

	// set name (same as storage name)
	auto name = stmt_expect(self, KNAME);
	volume_set_name(volume, &name->string);

	// generate volume id
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
	volume_set_id(volume, &id);

	// [(options)]
	if (stmt_if(self, '(') && !stmt_if(self, ')'))
		parse_volume_options(self, volume);

	return volume;
}

void
parse_volumes(Stmt* self, VolumeMgr* volumes)
{
	// [ON STORAGE storage, ...]

	// [ON]
	if (! stmt_if(self, KON))
	{
		// add main storage by default
		Str main;
		str_set(&main, "main", 4);

		auto volume = volume_allocate();
		volume_set_name(volume, &main);
		Uuid id;
		uuid_init(&id);
		uuid_generate(&id, &runtime()->random);
		volume_set_id(volume, &id);
		volume_mgr_add(volumes, volume);
		return;
	}

	// STORAGE
	stmt_expect(self, KSTORAGE);

	for (;;)
	{
		auto name = stmt_expect(self, KNAME);
		stmt_push(self, name);

		auto volume = volume_mgr_find(volumes, &name->string);
		if (volume)
			stmt_error(self, name, "storage used twice");

		volume = parse_volume(self);
		volume_mgr_add(volumes, volume);

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}
}
