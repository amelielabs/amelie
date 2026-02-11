
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
parse_index_using(Stmt* self, IndexConfig* index_config)
{
	// [USING type]
	if (! stmt_if(self, KUSING))
		return;
	auto type = stmt_next_shadow(self);
	if (type->id != KNAME)
		stmt_error(self, type, "index type expected");

	// tree | hash
	if (str_is_case(&type->string, "tree", 4))
		index_config_set_type(index_config, INDEX_TREE);
	else
	if (str_is_case(&type->string, "hash", 4))
		index_config_set_type(index_config, INDEX_HASH);
	else
		stmt_error(self, type, "unrecognized index type");
}

void
parse_index_create(Stmt* self, bool unique)
{
	// CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table_name (keys)
	// [USING type]
	auto stmt = ast_index_create_allocate();
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

	// find table
	auto table = table_mgr_find(&share()->db->catalog.table_mgr,
	                            self->parser->db,
	                            &stmt->table_name,
	                            false);
	if (! table)
		stmt_error(self, target, "table not found");

	// todo: unique indexes can be created only with 1 partition table
	if (unique && engine_main(&table->engine)->list_count != 1)
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

	// [USING type]
	parse_index_using(self, stmt->config);
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

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;
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

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_new = name->string;
}
