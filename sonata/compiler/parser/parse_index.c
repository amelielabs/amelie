
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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

		if (str_compare_raw(&key->string, "type", 4))
		{
			// =
			if (! stmt_if(self, '='))
				error("WITH (type <=> expected");

			// string
			auto value = stmt_if(self, KSTRING);
			if (! value)
				error("WITH (type = <string>) expected");

			// tree | hash
			if (str_compare_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_compare_cstr(&value->string, "hash"))
				index_config_set_type(index_config, INDEX_HASH);
			else
				error("WITH: unknown primary index type");

		} else {
			error("<%.*s> unrecognized parameter",
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
parse_index_create(Stmt* self)
{
	// CREATE INDEX [IF NOT EXISTS] name ON table_name (keys)
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
	Str table_schema;
	Str table_name;
	if (! parse_target(self, &table_schema, &table_name))
		error("CREATE INDEX name ON <name> expected");

	// find table
	auto table = table_mgr_find(&self->db->table_mgr, &table_schema,
	                            &table_name, true);

	// create index config
	auto config = index_config_allocate(table_columns(table));
	stmt->config = config;
	index_config_set_name(config, &name->string);
	index_config_set_primary(config, false);
	index_config_set_type(config, INDEX_TREE);
	keys_set_primary(&config->keys, table_keys(table));

	// (keys)
	parse_key(self, &config->keys);

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
