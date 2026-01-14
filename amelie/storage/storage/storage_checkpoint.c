
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
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_partition.h>
#include <amelie_tier.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

static Buf*
storage_checkpoint_catalog_dump(void* arg)
{
	// { databases, tables, udfs }
	Catalog* self = arg;
	auto buf = buf_create();
	encode_obj(buf);

	encode_raw(buf, "databases", 9);
	db_mgr_dump(&self->db_mgr, buf);

	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	encode_raw(buf, "udfs", 4);
	udf_mgr_dump(&self->udf_mgr, buf);

	encode_obj_end(buf);
	return buf;
}

enum
{
	RESTORE_DB,
	RESTORE_TABLE,
	RESTORE_UDF
};

static void
restore_replay(Storage* self, Tr* tr, int type, uint8_t** pos)
{
	switch (type) {
	case RESTORE_DB:
	{
		// read db config
		auto config = db_config_read(pos);
		defer(db_config_free, config);

		// create db
		db_mgr_create(&self->catalog.db_mgr, tr, config, false);
		break;
	}
	case RESTORE_TABLE:
	{
		// read table config
		auto config = table_config_read(pos);
		defer(table_config_free, config);

		// create table
		table_mgr_create(&self->catalog.table_mgr, tr, config, false);
		break;
	}
	case RESTORE_UDF:
	{
		// read udf config
		auto config = udf_config_read(pos);
		defer(udf_config_free, config);

		// create udf
		auto catalog = &self->catalog;
		udf_mgr_create(&catalog->udf_mgr, tr, config, false);

		auto udf = udf_mgr_find(&catalog->udf_mgr, &config->db, &config->name, true);
		catalog->iface->udf_compile(catalog, udf);
		break;
	}
	}
}

static void
restore_object(Storage* self, int type, uint8_t** pos)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	if (error_catch
	(
		// start transaction
		tr_begin(&tr);

		restore_replay(self, &tr, type, pos);

		// commit
		tr_commit(&tr);
	)) {
		tr_abort(&tr);
		rethrow();
	}
}

static void
storage_checkpoint_catalog_restore(uint8_t** pos, void* arg)
{
	// { databases, tables }
	Storage* self = arg;
	uint8_t* pos_databases = NULL;
	uint8_t* pos_tables    = NULL;
	uint8_t* pos_udfs      = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "databases", &pos_databases },
		{ DECODE_ARRAY, "tables",    &pos_tables    },
		{ DECODE_ARRAY, "udfs",      &pos_udfs      },
		{ 0,             NULL,        NULL          },
	};
	decode_obj(obj, "catalog", pos);

	// databases
	json_read_array(&pos_databases);
	while (! json_read_array_end(&pos_databases))
		restore_object(self, RESTORE_DB, &pos_databases);

	// tables
	json_read_array(&pos_tables);
	while (! json_read_array_end(&pos_tables))
		restore_object(self, RESTORE_TABLE, &pos_tables);

	// udfs
	json_read_array(&pos_udfs);
	while (! json_read_array_end(&pos_udfs))
		restore_object(self, RESTORE_UDF, &pos_udfs);
}

static void
storage_checkpoint_add(Checkpoint* cp, void* arg)
{
	Storage* self = arg;
	list_foreach(&self->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			checkpoint_add(cp, part);
		}
	}
}

static void
storage_checkpoint_complete(void* arg)
{
	Storage* self = arg;
	checkpoint_mgr_gc(&self->checkpoint_mgr);
	wal_mgr_gc(&self->wal_mgr);
}

CheckpointIf storage_checkpoint_if =
{
	.catalog_dump    = storage_checkpoint_catalog_dump,
	.catalog_restore = storage_checkpoint_catalog_restore,
	.add             = storage_checkpoint_add,
	.complete        = storage_checkpoint_complete
};
