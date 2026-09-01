
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
parse_index_using(Stmt* self, IndexConfig* config)
{
	// [USING type]
	if (! stmt_if(self, KUSING))
		return;
	auto type = stmt_next_shadow(self);
	if (type->id != KNAME)
		stmt_error(self, type, "index type expected");

	// tree | hash
	if (str_is_case(&type->string, "tree", 4))
		index_config_set_type(config, INDEX_TREE);
	else
	if (str_is_case(&type->string, "hash", 4))
		index_config_set_type(config, INDEX_HASH);
	else
		stmt_error(self, type, "unrecognized index type");

	// [(options)]
	if (!stmt_if(self, '(') || stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name value
		auto name = stmt_expect(self, KNAME);

		// size value
		if (str_is(&name->string, "size", 4))
		{
			auto value = stmt_expect(self, KINT);
			if (value->integer <= 0)
				stmt_error(self, value, "invalid index size");
			index_config_set_size(config, value->integer);
		} else {
			stmt_error(self, name, "unknown index option");
		}

		// )
		if (stmt_if(self, ')'))
			break;

		// ,
		stmt_expect(self, ',');
	}
}

void
parse_index_size(Stmt* self, IndexConfig* config, int partitions)
{
	unused(self);
	if (! config->size)
		return;

	// calculate size per partition
	uint64_t size = (config->size + partitions - 1) / partitions;

	// get next power of two rounding
	if (size < 64)
		size = 64;
	else
	if (size > 1ULL << 32)
		size = 1ULL << 32;
	else
		size = 1ULL << (64 - __builtin_clzll(size - 1));

	// set size per partition
	index_config_set_size(config, size);
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

	// [user.]name
	auto target = parse_target(self, &stmt->table_user, &stmt->table_name);

	// find table
	auto table = catalog_find_table(&share()->db->catalog, &stmt->table_user,
	                                &stmt->table_name,
	                                false);
	if (! table)
		stmt_error(self, target, "table not found");

	// create index config
	auto config = index_config_allocate(table_columns(table));
	stmt->config = config;
	index_config_set_name(config, &name->string);
	index_config_set_unique(config, unique);
	index_config_set_primary(config, false);
	index_config_set_type(config, INDEX_TREE);

	// (keys)
	parse_key(self, &config->keys, false);

	auto primary = table_primary(table);
	if (config->unique)
	{
		if (table->parts.list_count == 1)
		{
			// any keys allowed
		} else
		{
			// ensure all partitioning keys are explicitly made part of the
			// secondary index key
			auto primary = table_primary(table);
			for (auto at = 0; at < config->keys.count; at++)
			{
				auto key   = keys_at(&config->keys, at);
				auto match = false;
				for (auto at = 0; at < primary->keys.count; at++)
				{
					auto ref = keys_at(&primary->keys, at);
					if (ref->partitioning && ref->column == key->column)
					{
						match = true;
						break;
					}
				}
				if (! match)
					stmt_error(self, target, "secondary UNIQUE INDEX must include partitioning keys");
			}
		}

	} else
	{
		// copy primary keys, which are not already present
		keys_copy_distinct(&config->keys, table_keys(table));
	}

	// mark all partitioning keys
	for (auto at = 0; at < config->keys.count; at++)
	{
		auto key = keys_at(&config->keys, at);
		for (auto at = 0; at < primary->keys.count; at++)
		{
			auto ref = keys_at(&primary->keys, at);
			if (ref->partitioning && ref->column == key->column)
				key->partitioning = true;
		}
	}

	// [USING type]
	parse_index_using(self, stmt->config);

	// configure index size according to the table partitions
	parse_index_size(self, stmt->config, table->config->parts_count);
}

void
parse_index_drop(Stmt* self)
{
	// DROP INDEX [IF EXISTS] name ON table_name
	auto stmt = ast_index_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// table
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// [user.]name
	parse_target(self, &stmt->table_user, &stmt->table_name);
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

	// [user.]name
	parse_target(self, &stmt->table_user, &stmt->table_name);

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_new = name->string;
}
