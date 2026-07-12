
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
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

enum
{
	RESTORE_USER,
	RESTORE_TABLE,
	RESTORE_CLONE,
	RESTORE_UDF,
	RESTORE_TOPIC,
	RESTORE_SUB
};

static void
catalog_restore_relation(Catalog* self, Tr* tr, int type, uint8_t** pos)
{
	Str name;
	str_set(&name, "amelie", 6);
	auto user = catalog_find_user(self, &name, false);
	if (user)
		tr_set_user(tr, &user->rel);

	switch (type) {
	case RESTORE_USER:
	{
		// read user config
		auto config = user_config_read(pos);
		defer(user_config_free, config);

		// create user
		user_create(self, tr, config, false);
		break;
	}
	case RESTORE_TABLE:
	{
		// read table config
		auto config = table_config_read(pos);
		defer(table_config_free, config);

		// create table
		table_create(self, tr, config, false);
		break;
	}
	case RESTORE_CLONE:
	{
		// read clone config
		auto config = clone_config_read(pos);
		defer(clone_config_free, config);

		// create clone
		clone_create(self, tr, config, false);
		break;
	}
	case RESTORE_UDF:
	{
		// read udf config
		auto config = udf_config_read(pos);
		defer(udf_config_free, config);

		// create udf
		udf_create(self, tr, config, false);
		break;
	}
	case RESTORE_TOPIC:
	{
		// read topic config
		auto config = topic_config_read(pos);
		defer(topic_config_free, config);

		// create topic
		topic_create(self, tr, config, false);
		break;
	}
	case RESTORE_SUB:
	{
		// read subscription config
		auto config = sub_config_read(pos);
		defer(sub_config_free, config);

		// create subscription
		sub_create(self, tr, config, false);
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
	// { lsn, users, tables, clones, udfs, topics, subs }
	int64_t  lsn        = 0;
	uint8_t* pos_users  = NULL;
	uint8_t* pos_tables = NULL;
	uint8_t* pos_clones = NULL;
	uint8_t* pos_udfs   = NULL;
	uint8_t* pos_topics = NULL;
	uint8_t* pos_subs   = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "lsn",    &lsn          },
		{ DECODE_ARRAY, "users",  &pos_users    },
		{ DECODE_ARRAY, "tables", &pos_tables   },
		{ DECODE_ARRAY, "clones", &pos_clones   },
		{ DECODE_ARRAY, "udfs",   &pos_udfs     },
		{ DECODE_ARRAY, "topics", &pos_topics   },
		{ DECODE_ARRAY, "subs",   &pos_subs     },
		{ 0,             NULL,     NULL         },
	};
	decode_obj(obj, "catalog", pos);

	// users
	unpack_array(&pos_users);
	while (! unpack_array_end(&pos_users))
		catalog_restore_object(self, RESTORE_USER, &pos_users);

	// tables
	unpack_array(&pos_tables);
	while (! unpack_array_end(&pos_tables))
		catalog_restore_object(self, RESTORE_TABLE, &pos_tables);

	// clones
	unpack_array(&pos_clones);
	while (! unpack_array_end(&pos_clones))
		catalog_restore_object(self, RESTORE_CLONE, &pos_clones);

	// udfs
	unpack_array(&pos_udfs);
	while (! unpack_array_end(&pos_udfs))
		catalog_restore_object(self, RESTORE_UDF, &pos_udfs);

	// topics
	unpack_array(&pos_topics);
	while (! unpack_array_end(&pos_topics))
		catalog_restore_object(self, RESTORE_TOPIC, &pos_topics);

	// subs
	unpack_array(&pos_subs);
	while (! unpack_array_end(&pos_subs))
		catalog_restore_object(self, RESTORE_SUB, &pos_subs);

	// set catalog lsn
	state_lsn_follow(lsn);

	opt_int_set(&state()->checkpoint, lsn);
}

void
catalog_read(Catalog* self, char* path)
{
	// read catalog file
	auto buf = file_import("{s}", path);
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
catalog_state(Catalog* self, uint64_t lsn)
{
	// { lsn, users, tables, clones, udfs, topics, subs }
	auto buf = buf_create();
	encode_obj(buf);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, lsn);

	// users
	encode_raw(buf, "users", 5);
	rels_dump(&self->users, REL_USER, buf, 0);

	// tables
	encode_raw(buf, "tables", 6);
	rels_dump(&self->rels, REL_TABLE, buf, 0);

	// clones
	encode_raw(buf, "clones", 6);
	rels_dump(&self->rels, REL_CLONE, buf, 0);

	// udfs
	encode_raw(buf, "udfs", 4);
	rels_dump(&self->rels, REL_UDF, buf, 0);

	// topics
	encode_raw(buf, "topics", 6);
	rels_dump(&self->rels, REL_TOPIC, buf, 0);

	// subs
	encode_raw(buf, "subs", 4);
	rels_dump(&self->rels, REL_SUBSCRIPTION, buf, 0);

	encode_obj_end(buf);
	return buf;
}

void
catalog_write(Catalog* self, char* path)
{
	unused(self);

	// prepare catalog dump
	auto lsn  = state_lsn();
	auto data = catalog_state(self, lsn);
	defer_buf(data);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	auto pos = data->start;
	json_export_pretty(&text, NULL, &pos);

	// create catalog.json
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);

	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);
}
