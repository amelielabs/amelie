
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_with(Stmt* self, IndexConfig* index_config)
{
	// [WITH]
	if (! stmt_if(self, KWITH))
		return;

	// (
	if (! stmt_if(self, '('))
		error("WITH <(> expected");

	for (;;)
	{
		// key
		auto key = stmt_if(self, KNAME);
		if (! key)
			error("WITH (<name> expected");

		if (str_is(&key->string, "type", 4))
		{
			// =
			if (! stmt_if(self, '='))
				error("WITH (type <=> expected");

			// string
			auto value = stmt_if(self, KSTRING);
			if (! value)
				error("WITH (type = <string>) expected");

			// tree | hash
			if (str_is_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_is_cstr(&value->string, "hash"))
				index_config_set_type(index_config, INDEX_HASH);
			else
				error("WITH: unknown primary index type");

		} else {
			error("WITH: <%.*s> unrecognized parameter",
			      str_size(&key->string), str_of(&key->string));
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (stmt_if(self, ')'))
			break;
	}
}

void
parse_index_create(Stmt* self, bool unique)
{
	// CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table_name (keys)
	// [WITH (...)]
	auto stmt = ast_index_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("CREATE INDEX <name> expected");

	// ON
	if (! stmt_if(self, KON))
		error("CREATE INDEX name <ON> expected");

	// [schema.table_name]
	if (! parse_target(self, &stmt->table_schema, &stmt->table_name))
		error("CREATE INDEX name ON <name> expected");

	// find table
	auto table = table_mgr_find(&self->db->table_mgr, &stmt->table_schema,
	                            &stmt->table_name, true);

	// ensure table is shared for unique index
	if (unique && !table->config->shared)
		error("CREATE UNIQUE INDEX cannot be created on distributed tables");

	// create index config
	auto config = index_config_allocate(table_columns(table));
	stmt->config = config;
	index_config_set_name(config, &name->string);
	index_config_set_unique(config, unique);
	index_config_set_primary(config, false);
	index_config_set_type(config, INDEX_TREE);

	// (keys)
	parse_key(self, &config->keys);

	// copy primary keys, which are not already present (non unique support)
	if (! config->unique)
		keys_copy_distinct(&config->keys, table_keys(table));

	// [WITH (options)]
	parse_with(self, stmt->config);
}

void
parse_index_drop(Stmt* self)
{
	// DROP INDEX [IF EXISTS] name ON table_name
	auto stmt = ast_index_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("DROP INDEX <name> expected");
	stmt->name = name->string;

	// ON
	if (! stmt_if(self, KON))
		error("DROP INDEX name <ON> expected");

	// [schema.table_name]
	if (! parse_target(self, &stmt->table_schema, &stmt->table_name))
		error("DROP INDEX name ON <name> expected");
}

void
parse_index_alter(Stmt* self)
{
	// ALTER INDEX [IF EXISTS] name ON table_name RENAME TO name
	auto stmt = ast_index_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("ALTER INDEX <name> expected");
	stmt->name = name->string;

	// ON
	if (! stmt_if(self, KON))
		error("ALTER INDEX name <ON> expected");

	// [schema.table_name]
	if (! parse_target(self, &stmt->table_schema, &stmt->table_name))
		error("ALTER INDEX name ON <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER INDEX name ON name <RENAME> expected");

	// TO
	if (! stmt_if(self, KTO))
		error("ALTER INDEX name ON name RENAME <TO> expected");

	// name
	name = stmt_if(self, KNAME);
	if (! name)
		error("ALTER INDEX name ON name RENAME TO <name> expected");
	stmt->name_new = name->string;
}
