
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
dst_validate_table(DstUser* self)
{
	auto rel = &self->rels[DST_REL_TABLE];
	auto client = self->client;

	// table
	dst_execute(self->dst, client, false, "SELECT * FROM dst_table");
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
			error("dst_table: key {u64} is missing", ref->key);
		} else
		{
			if (ref->value != value)
				error("dst_table: key {u64} value expected '{u64}' got '{u64}'",
				      ref->key, ref->value, value);
		}
		count++;
	}
	if (count != rel->state.count)
		error("dst_table: keys count expected '{d}' got '{d}'",
		      count, rel->state.count);
}

static void
dst_validate_table_vector(DstUser* self)
{
	auto rel = &self->rels[DST_REL_TABLE_VECTOR];
	auto client = self->client;

	// table
	dst_execute(self->dst, client, false, "SELECT * FROM dst_table_vector");
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
			error("dst_table_vector: key {u64} is missing", ref->key);
		} else
		{
			if (!float_compare(ref->value_vector[0], vector[0], 1e-6f) ||
			    !float_compare(ref->value_vector[1], vector[1], 1e-6f) ||
			    !float_compare(ref->value_vector[2], vector[2], 1e-6f) ||
			    !float_compare(ref->value_vector[3], vector[3], 1e-6f))
				error("dst_table_vector: key {u64} vector does not match",
				      ref->key);
		}
		count++;
	}
	if (count != rel->state.count)
		error("dst_table_vector: keys count expected '{d}' got {d}'",
		      count, rel->state.count);
}

static uint64_t
dst_validate_sub_table(DstUser* self)
{
	auto rel = &self->rels[DST_REL_TABLE];
	auto client = self->client;

	// table
	dst_execute(self->dst, client, false,
	            "SELECT count(*), sum(row.id::int), max(lsn) FROM dst_sub_table");
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

	// [[count, sum, lsn]]
	unpack_array(&rows);
	unpack_array(&rows);
	int64_t count;
	int64_t sum;
	int64_t lsn;
	unpack_int(&rows, &count);
	unpack_int(&rows, &sum);
	unpack_int(&rows, &lsn);
	unpack_array_end(&rows);
	unpack_array_end(&rows);

	// validate
	if (count != rel->cdc_count)
		error("dst_sub_table: count mismatch expected {i64} got {i64}",
		      count, rel->cdc_count);

	if (sum != rel->cdc_sum)
		error("dst_sub_table: keys sum mismatch");

	return lsn;
}

static uint64_t
dst_validate_sub_topic(DstUser* self)
{
	auto rel = &self->rels[DST_REL_TOPIC];
	auto client = self->client;

	// topic
	dst_execute(self->dst, client, false,
	            "SELECT count(*), sum(row[0]::int), max(lsn) FROM dst_sub_topic");
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

	// [[count, sum, lsn]]
	unpack_array(&rows);
	unpack_array(&rows);
	int64_t count;
	int64_t sum;
	int64_t lsn;
	unpack_int(&rows, &count);
	unpack_int(&rows, &sum);
	unpack_int(&rows, &lsn);
	unpack_array_end(&rows);
	unpack_array_end(&rows);

	// validate
	if (count != rel->cdc_count)
		error("dst_sub_topic: count mismatch expected {i64} got {i64}",
		      count, rel->cdc_count);

	if (sum != rel->cdc_sum)
		error("dst_sub_topic: keys sum mismatch");

	return lsn;
}

void
dst_validate_user(DstUser* self)
{
	// dst_table
	dst_validate_table(self);

	// dst_table_vector
	dst_validate_table_vector(self);

	// dst_sub_table
	auto ack_table = dst_validate_sub_table(self);

	// todo: sub table_vector

	// dst_sub_topic
	auto ack_topic = dst_validate_sub_topic(self);

	// ack dst_sub_table
	if (ack_table)
	{
		dst_execute(self->dst, self->client, false,
		            "ACKNOWLEDGE dst_sub_table TO {u64}, 10",
		            ack_table);
		auto rel = &self->rels[DST_REL_TABLE];
		rel->cdc_sum   = 0;
		rel->cdc_count = 0;
	}

	// ack dst_sub_table_vector
	if (1)
	{
		auto rel = &self->rels[DST_REL_TABLE_VECTOR];
		rel->cdc_sum   = 0;
		rel->cdc_count = 0;
	}

	// ack dst_sub_topic
	if (ack_topic)
	{
		dst_execute(self->dst, self->client, false,
		            "ACKNOWLEDGE dst_sub_topic TO {u64}, 10",
		            ack_topic);
		auto rel = &self->rels[DST_REL_TOPIC];
		rel->cdc_sum   = 0;
		rel->cdc_count = 0;
	}
}

void
dst_validate(Dst* self)
{
	for (auto i = 0; i < self->users_count; i++)
		dst_validate_user(&self->users[i]);

	info("[{u64}] OK", self->step);
}
