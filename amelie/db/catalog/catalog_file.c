
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
#include <amelie_part.h>
#include <amelie_catalog.h>

enum
{
	RESTORE_USER,
	RESTORE_STORAGE,
	RESTORE_TABLE,
	RESTORE_BRANCH,
	RESTORE_UDF,
	RESTORE_TOPIC
};

static void
catalog_restore_relation(Catalog* self, Tr* tr, int type, uint8_t** pos)
{
	switch (type) {
	case RESTORE_USER:
	{
		// read user config
		auto config = user_config_read(pos);
		defer(user_config_free, config);

		// create user
		user_mgr_create(&self->user_mgr, tr, config, false);
		break;
	}
	case RESTORE_STORAGE:
	{
		// read storage config
		auto config = storage_config_read(pos);
		defer(storage_config_free, config);

		// create storage
		storage_mgr_create(&self->storage_mgr, tr, config, false);
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
	case RESTORE_BRANCH:
	{
		// read branch config
		auto config = branch_config_read(pos);
		defer(branch_config_free, config);

		// create branch
		branch_mgr_create(&self->branch_mgr, tr, config, false);
		break;
	}
	case RESTORE_UDF:
	{
		// read udf config
		auto config = udf_config_read(pos);
		defer(udf_config_free, config);

		// create udf
		udf_mgr_create(&self->udf_mgr, tr, config, false);

		auto udf = udf_mgr_find(&self->udf_mgr, &config->user, &config->name, true);
		self->iface->udf_compile(self, udf);
		break;
	}
	case RESTORE_TOPIC:
	{
		// read topic config
		auto config = topic_config_read(pos);
		defer(topic_config_free, config);

		// create topic
		topic_mgr_create(&self->topic_mgr, tr, config, false);

		// todo: attach
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
	// { lsn, tsn, users, storages, tables, branches, udfs, topics }
	int64_t  lsn          = 0;
	int64_t  tsn          = 0;
	uint8_t* pos_users    = NULL;
	uint8_t* pos_storages = NULL;
	uint8_t* pos_tables   = NULL;
	uint8_t* pos_branches = NULL;
	uint8_t* pos_udfs     = NULL;
	uint8_t* pos_topics   = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "lsn",      &lsn          },
		{ DECODE_INT,   "tsn",      &tsn          },
		{ DECODE_ARRAY, "users",    &pos_users    },
		{ DECODE_ARRAY, "storages", &pos_storages },
		{ DECODE_ARRAY, "tables",   &pos_tables   },
		{ DECODE_ARRAY, "branches", &pos_branches },
		{ DECODE_ARRAY, "udfs",     &pos_udfs     },
		{ DECODE_ARRAY, "topics",   &pos_topics   },
		{ 0,             NULL,       NULL         },
	};
	decode_obj(obj, "catalog", pos);

	// users
	json_read_array(&pos_users);
	while (! json_read_array_end(&pos_users))
		catalog_restore_object(self, RESTORE_USER, &pos_users);

	// storages
	json_read_array(&pos_storages);
	while (! json_read_array_end(&pos_storages))
		catalog_restore_object(self, RESTORE_STORAGE, &pos_storages);

	// tables
	json_read_array(&pos_tables);
	while (! json_read_array_end(&pos_tables))
		catalog_restore_object(self, RESTORE_TABLE, &pos_tables);

	// branches
	json_read_array(&pos_branches);
	while (! json_read_array_end(&pos_branches))
		catalog_restore_object(self, RESTORE_BRANCH, &pos_branches);

	// udfs
	json_read_array(&pos_udfs);
	while (! json_read_array_end(&pos_udfs))
		catalog_restore_object(self, RESTORE_UDF, &pos_udfs);

	// topics
	json_read_array(&pos_topics);
	while (! json_read_array_end(&pos_topics))
		catalog_restore_object(self, RESTORE_TOPIC, &pos_topics);

	// set catalog lsn and tsn
	opt_int_set(&state()->catalog, lsn);
	opt_int_set(&state()->catalog_pending, lsn);

	state_lsn_follow(lsn);
	state_tsn_follow(tsn);
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

Buf*
catalog_write_prepare(Catalog* self, uint64_t lsn, uint64_t tsn)
{
	// { lsn, tsn, users, storages, tables, branches, udfs, topics }
	auto buf = buf_create();
	encode_obj(buf);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, lsn);

	// tsn
	encode_raw(buf, "tsn", 3);
	encode_integer(buf, tsn);

	// users
	encode_raw(buf, "users", 5);
	user_mgr_dump(&self->user_mgr, buf);

	// storages
	encode_raw(buf, "storages", 8);
	storage_mgr_dump(&self->storage_mgr, buf);

	// tables
	encode_raw(buf, "tables", 6);
	table_mgr_dump(&self->table_mgr, buf);

	// branches
	encode_raw(buf, "branches", 8);
	branch_mgr_dump(&self->branch_mgr, buf);

	// udfs
	encode_raw(buf, "udfs", 4);
	udf_mgr_dump(&self->udf_mgr, buf);

	// topics
	encode_raw(buf, "topics", 6);
	topic_mgr_dump(&self->topic_mgr, buf);

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
	auto tsn  = state_tsn();
	auto data = catalog_write_prepare(self, lsn, tsn);
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
