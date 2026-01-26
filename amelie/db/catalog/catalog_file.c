
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_partition.h>
#include <amelie_catalog.h>

enum
{
	RESTORE_STORAGE,
	RESTORE_DATABASE,
	RESTORE_TABLE,
	RESTORE_UDF
};

static void
catalog_restore_relation(Catalog* self, Tr* tr, int type, uint8_t** pos)
{
	switch (type) {
	case RESTORE_STORAGE:
	{
		// read storage config
		auto config = storage_config_read(pos);
		defer(storage_config_free, config);

		// create storage
		storage_mgr_create(&self->storage_mgr, tr, config, false);
		break;
	}
	case RESTORE_DATABASE:
	{
		// read db config
		auto config = database_config_read(pos);
		defer(database_config_free, config);

		// create db
		database_mgr_create(&self->db_mgr, tr, config, false);
		break;
	}
	case RESTORE_TABLE:
	{
		// read table config
		auto config = table_config_read(pos);
		defer(table_config_free, config);

		// create table
		table_mgr_create(&self->table_mgr, tr, config, false);
		break;
	}
	case RESTORE_UDF:
	{
		// read udf config
		auto config = udf_config_read(pos);
		defer(udf_config_free, config);

		// create udf
		udf_mgr_create(&self->udf_mgr, tr, config, false);

		auto udf = udf_mgr_find(&self->udf_mgr, &config->db, &config->name, true);
		self->iface->udf_compile(self, udf);
		break;
	}
	}
}

static void
catalog_restore_object(Catalog* self, int type, uint8_t** pos)
{
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	if (error_catch
	(
		// start transaction
		tr_begin(&tr);

		catalog_restore_relation(self, &tr, type, pos);

		// commit (without wal write)
		tr_commit(&tr);
	)) {
		tr_abort(&tr);
		rethrow();
	}
}

static void
catalog_restore(Catalog* self, uint8_t** pos)
{
	// { lsn, storages, databases, tables, udfs }
	int64_t  lsn           = 0;
	uint8_t* pos_storages  = NULL;
	uint8_t* pos_databases = NULL;
	uint8_t* pos_tables    = NULL;
	uint8_t* pos_udfs      = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "lsn",       &lsn           },
		{ DECODE_ARRAY, "storages",  &pos_storages  },
		{ DECODE_ARRAY, "databases", &pos_databases },
		{ DECODE_ARRAY, "tables",    &pos_tables    },
		{ DECODE_ARRAY, "udfs",      &pos_udfs      },
		{ 0,             NULL,        NULL          },
	};
	decode_obj(obj, "catalog", pos);

	// storages
	json_read_array(&pos_databases);
	while (! json_read_array_end(&pos_storages))
		catalog_restore_object(self, RESTORE_STORAGE, &pos_storages);

	// databases
	json_read_array(&pos_databases);
	while (! json_read_array_end(&pos_databases))
		catalog_restore_object(self, RESTORE_DATABASE, &pos_databases);

	// tables
	json_read_array(&pos_tables);
	while (! json_read_array_end(&pos_tables))
		catalog_restore_object(self, RESTORE_TABLE, &pos_tables);

	// udfs
	json_read_array(&pos_udfs);
	while (! json_read_array_end(&pos_udfs))
		catalog_restore_object(self, RESTORE_UDF, &pos_udfs);

	// set catalog lsn
	opt_int_set(&state()->catalog, lsn);
	opt_int_set(&state()->catalog_pending, lsn);
}

void
catalog_read(Catalog* self)
{
	// restore catalog.json from incomplete, if possible
	char path[PATH_MAX];
	if (! fs_exists("%s/catalog.json", state_directory()))
	{
		// check if catalog.json.incomplete exists
		if (! fs_exists("%s/catalog.json.incomplete", state_directory()))
			error("catalog: catalog file is missing");

		// restore catalog file
		sfmt(path, sizeof(path), "%s/catalog.json.incomplete",
			 state_directory());
		fs_rename(path, "%s/catalog.json", state_directory());
		// todo: sync dir
	} else
	{
		// remove catalog.json.incomplete
		if (fs_exists("%s/catalog.json.incomplete", state_directory()))
			fs_unlink("%s/catalog.json.incomplete", state_directory());
	}

	// read catalog file
	auto buf = file_import("%s/catalog.json", state_directory());
	defer_buf(buf);
	Str text;
	buf_str(buf, &text);

	// convert json
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);
	auto pos = json.buf->start;

	// restore catalog objects
	catalog_restore(self, &pos);
}

static Buf*
catalog_write_prepare(Catalog* self, uint64_t lsn)
{
	// { lsn, storages, databases, tables, udfs }
	auto buf = buf_create();
	encode_obj(buf);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, lsn);

	// storages
	encode_raw(buf, "storages", 8);
	storage_mgr_dump(&self->storage_mgr, buf);

	// databases
	encode_raw(buf, "databases", 9);
	database_mgr_dump(&self->db_mgr, buf);

	// tables
	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	// udfs
	encode_raw(buf, "udfs", 4);
	udf_mgr_dump(&self->udf_mgr, buf);

	encode_obj_end(buf);
	return buf;
}

void
catalog_write(Catalog* self)
{
	unused(self);

	// remove catalog.json.incomplete file, if exists
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/catalog.json.incomplete",
	     state_directory());

	if (fs_exists("%s", path))
		fs_unlink("%s", path);

	// prepare catalog dump
	auto lsn  = state_catalog_pending();
	auto data = catalog_write_prepare(self, lsn);
	defer_buf(data);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	auto pos = data->start;
	json_export_pretty(&text, NULL, &pos);

	// create catalog.json.incomplete
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);

	// todo: sync dir
	if (opt_int_of(&config()->catalog_sync))
		file_sync(&file);

	// remove catalog.json file, if exsists
	sfmt(path, sizeof(path), "%s/catalog.json",
	     state_directory());

	if (fs_exists("%s", path))
		fs_unlink("%s", path);

	// rename
	sfmt(path, sizeof(path), "%s/catalog.json.incomplete",
	     state_directory());
	fs_rename(path, "%s/catalog.json", state_directory());
	// todo: sync dir

	// update catalog lsn
	opt_int_set(&state()->catalog, lsn);
}
