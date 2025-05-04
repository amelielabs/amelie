
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

static void
parse_with(Stmt* self, IndexConfig* index_config)
{
	// [WITH]
	if (! stmt_if(self, KWITH))
		return;

	// (
	stmt_expect(self, '(');

	for (;;)
	{
		// key
		auto key = stmt_expect(self, KNAME);
		if (str_is(&key->string, "type", 4))
		{
			// =
			stmt_expect(self, '=');

			// string
			auto value = stmt_expect(self, KSTRING);

			// tree | hash
			if (str_is_cstr(&value->string, "tree"))
				index_config_set_type(index_config, INDEX_TREE);
			else
			if (str_is_cstr(&value->string, "hash"))
				index_config_set_type(index_config, INDEX_HASH);
			else
				stmt_error(self, value, "unrecognized index type");

		} else {
			stmt_error(self, key, "unrecognized parameter");
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
	auto name = stmt_expect(self, KNAME);

	// ON
	stmt_expect(self, KON);

	// [schema.table_name]
	auto target = parse_target(self, &stmt->table_schema, &stmt->table_name);
	if (! target)
		stmt_error(self, NULL, "table name expected");

	// find table
	auto table = table_mgr_find(&self->parser->db->table_mgr, &stmt->table_schema,
	                            &stmt->table_name, false);
	if (! table)
		stmt_error(self, target, "table not found");

	// todo: unique indexes can be created only with 1 partition table
	if (unique && table->part_list.list_count != 1)
		stmt_error(self, target, "secondary UNIQUE INDEX allowed only for tables with one partition");

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
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// [schema.table_name]
	if (! parse_target(self, &stmt->table_schema, &stmt->table_name))
		stmt_error(self, NULL, "table name expected");
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
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// [schema.table_name]
	if (! parse_target(self, &stmt->table_schema, &stmt->table_name))
		stmt_error(self, NULL, "table name expected");

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_new = name->string;
}
