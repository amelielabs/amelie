
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_dst.h>
#include "overflow_fp.h"

static void
dst_validate_table(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// table
	dst_execute(self->dst, client, "SELECT id, state FROM table_{u64}", rel->id);
	Str content;
	buf_str(&client->reply.content, &content);
	// info("{str}", &content);

	// parse json result
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &content, NULL);

	uint8_t* pos     = json.buf->start;
	uint8_t* columns = NULL;
	uint8_t* rows    = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "columns", &columns },
		{ DECODE_ARRAY, "rows",    &rows    },
		{ 0,             NULL,      NULL    },
	};
	decode_obj(obj, "result", &pos);

	// read and validate rows

	// [[...], ...]
	auto count = 0;
	unpack_array(&rows);
	while (! unpack_array_end(&rows))
	{
		// [key, value]
		unpack_array(&rows);
		int64_t key;
		int64_t value;
		unpack_int(&rows, &key);
		unpack_int(&rows, &value);
		unpack_array_end(&rows);

		auto ref = dst_rel_get(rel, key);
		if (! ref)
		{
			error("table_{u64}: key {u64} is missing",
			      rel->id, key);
		} else
		{
			if (ref->value != value)
				error("table_{u64}: key {u64} value expected '{i64}' got '{i64}'",
				      rel->id, ref->key, ref->value, value);
		}
		count++;
	}
	if (count != rel->state.count)
		error("table_{u64}: keys count expected '{d}' got '{d}'",
		      rel->id, rel->state.count, count);
}

static void
dst_validate_table_vector(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// table
	dst_execute(self->dst, client, "SELECT * FROM table_vector_{u64}", rel->id);
	Str content;
	buf_str(&client->reply.content, &content);
	//info("{str}", &content);

	// parse json result
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &content, NULL);

	uint8_t* pos     = json.buf->start;
	uint8_t* columns = NULL;
	uint8_t* rows    = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "columns", &columns },
		{ DECODE_ARRAY, "rows",    &rows    },
		{ 0,             NULL,      NULL    },
	};
	decode_obj(obj, "result", &pos);

	// read and validate rows

	// [[...], ...]
	auto count = 0;
	unpack_array(&rows);
	while (! unpack_array_end(&rows))
	{
		// [key, [vector]]
		unpack_array(&rows);
		int64_t key;
		unpack_int(&rows, &key);

		unpack_array(&rows);
		double vector[4];
		unpack_real(&rows, &vector[0]);
		unpack_real(&rows, &vector[1]);
		unpack_real(&rows, &vector[2]);
		unpack_real(&rows, &vector[3]);
		unpack_array_end(&rows);
		unpack_array_end(&rows);

		auto ref = dst_rel_get(rel, key);
		if (! ref)
		{
			error("table_vector_{u64}: key {u64} is missing",
			      rel->id, key);
		} else
		{
			if (!float_compare(ref->value_vector[0], vector[0], 1e-6f) ||
			    !float_compare(ref->value_vector[1], vector[1], 1e-6f) ||
			    !float_compare(ref->value_vector[2], vector[2], 1e-6f) ||
			    !float_compare(ref->value_vector[3], vector[3], 1e-6f))
				error("table_vector_{u64}: key {u64} vector does not match",
				      rel->id, ref->key);
		}
		count++;
	}
	if (count != rel->state.count)
		error("table_vector_{u64}: keys count expected '{d}' got {d}'",
		      rel->id, rel->state.count, count);
}

static void
dst_validate_index(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// index
	dst_execute(self->dst, client, "SELECT id, state FROM table_{u64} INDEX index_{u64}",
	            rel->parent->id, rel->id);
	Str content;
	buf_str(&client->reply.content, &content);
	// info("{str}", &content);

	// parse json result
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &content, NULL);

	uint8_t* pos     = json.buf->start;
	uint8_t* columns = NULL;
	uint8_t* rows    = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "columns", &columns },
		{ DECODE_ARRAY, "rows",    &rows    },
		{ 0,             NULL,      NULL    },
	};
	decode_obj(obj, "result", &pos);

	// read and validate rows

	// [[...], ...]
	auto count = 0;
	unpack_array(&rows);
	while (! unpack_array_end(&rows))
	{
		// [key, value]
		unpack_array(&rows);
		int64_t key;
		int64_t value;
		unpack_int(&rows, &key);
		unpack_int(&rows, &value);
		unpack_array_end(&rows);

		// using parent table state
		auto ref = dst_rel_get(rel->parent, key);
		if (! ref)
		{
			error("index_{u64}: key {u64} is missing",
			      rel->id, key);
		} else
		{
			if (ref->value != value)
				error("index_{u64}: key {u64} value expected '{i64}' got '{i64}'",
				      rel->id, ref->key, ref->value, value);
		}
		count++;
	}
	if (count != rel->parent->state.count)
		error("index_{u64}: keys count expected '{d}' got '{d}'",
		      rel->id, rel->parent->state.count, count);
}

static void
dst_validate_clone(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// clone
	dst_execute(self->dst, client, "SELECT id, state FROM clone_{u64}_{u64}",
	            rel->parent->id, rel->id);
	Str content;
	buf_str(&client->reply.content, &content);
	//info("{str}", &content);

	// parse json result
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &content, NULL);

	uint8_t* pos     = json.buf->start;
	uint8_t* columns = NULL;
	uint8_t* rows    = NULL;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "columns", &columns },
		{ DECODE_ARRAY, "rows",    &rows    },
		{ 0,             NULL,      NULL    },
	};
	decode_obj(obj, "result", &pos);

	// read and validate rows

	// [[...], ...]
	auto count = 0;
	unpack_array(&rows);
	while (! unpack_array_end(&rows))
	{
		// [key, value]
		unpack_array(&rows);
		int64_t key;
		int64_t value;
		unpack_int(&rows, &key);
		unpack_int(&rows, &value);
		unpack_array_end(&rows);

		auto ref = dst_rel_get(rel, key);
		if (! ref)
		{
			error("clone_{u64}_{u64}: key {u64} is missing",
			      rel->parent->id, rel->id, key);
		} else
		{
			if (ref->value != value)
				error("clone_{u64}_{u64}: key {u64} value expected '{i64}' got '{i64}'",
				      rel->parent->id, rel->id, ref->key, ref->value, value);
		}
		count++;
	}
	if (count != rel->state.count)
		error("clone_{u64}_{u64}: keys count expected '{d}' got '{d}'",
		      rel->parent->id, rel->id, rel->state.count, count);
}

void
dst_validate_user(DstUser* self)
{
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		switch (rel->type) {
		case DST_REL_TABLE:
		{
			// table
			dst_validate_table(self, rel);
			break;
		}
		case DST_REL_TABLE_VECTOR:
		{
			// table_vector
			dst_validate_table_vector(self, rel);
			break;
		}
		case DST_REL_INDEX:
		{
			// table index
			dst_validate_index(self, rel);
			break;
		}
		case DST_REL_CLONE:
		{
			dst_validate_clone(self, rel);
			break;
		}
		}
	}
}

void
dst_validate(Dst* self)
{
	dst_stat(&self->stats, DST_STAT_VALIDATION);

	list_foreach(&self->users)
	{
		auto user = list_at(DstUser, link);
		dst_validate_user(user);
	}

	info("[{u64}] OK", self->step);
}
