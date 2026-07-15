
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
	dst_execute(self->dst, client, "SELECT * FROM table_{u64}", rel->id);
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
			error("table_{u64}: key {u64} is missing",
			      rel->id, key);
		} else
		{
			if (ref->value != value)
				error("table_{u64}: key {u64} value expected '{u64}' got '{u64}'",
				      rel->id, ref->key, ref->value, value);
		}
		count++;
	}
	if (count != rel->state.count)
		error("table_{u64}: keys count expected '{d}' got '{d}'",
		      rel->id, count, rel->state.count);
}

static uint64_t
dst_validate_table_sub(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// table
	dst_execute(self->dst, client,
	            "SELECT count(*), sum(row.id::int), max(lsn) FROM table_sub_{u64}",
	            rel->id);
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
	int64_t sum   = 0;
	int64_t lsn   = 0;
	unpack_int(&rows, &count);
	if (count == 0)
	{
		unpack_null(&rows);
		unpack_null(&rows);
	} else
	{
		unpack_int(&rows, &sum);
		unpack_int(&rows, &lsn);
	}
	unpack_array_end(&rows);
	unpack_array_end(&rows);

	// validate
	if (count != rel->cdc_count)
		error("table_sub_{u64}: count mismatch expected {i64} got {i64}",
		      rel->id, count, rel->cdc_count);

	if (sum != rel->cdc_sum)
		error("table_sub_{u64}: keys sum mismatch",
		      rel->id);

	return lsn;
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
		      rel->id, count, rel->state.count);
}

static uint64_t
dst_validate_topic_sub(DstUser* self, DstRel* rel)
{
	auto client = self->client;

	// topic
	dst_execute(self->dst, client,
	            "SELECT count(*), sum(row[0]::int), max(lsn) FROM topic_sub_{u64}",
	            rel->id);
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
	int64_t sum   = 0;
	int64_t lsn   = 0;
	unpack_int(&rows, &count);
	if (count == 0)
	{
		unpack_null(&rows);
		unpack_null(&rows);
	} else
	{
		unpack_int(&rows, &sum);
		unpack_int(&rows, &lsn);
	}
	unpack_array_end(&rows);
	unpack_array_end(&rows);

	// validate
	if (count != rel->cdc_count)
		error("topic_sub_{u64}: count mismatch expected {i64} got {i64}",
		      rel->id, count, rel->cdc_count);

	if (sum != rel->cdc_sum)
		error("topic_sub_{u64}: keys sum mismatch",
		      rel->id);

	return lsn;
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

			// subscription
			auto ack = dst_validate_table_sub(self, rel);
			if (ack)
			{
				dst_execute(self->dst, self->client,
				            "ACKNOWLEDGE table_sub_{u64} TO {u64}, 10",
				             rel->id, ack);
				rel->cdc_sum   = 0;
				rel->cdc_count = 0;
			}
			break;
		}
		case DST_REL_TABLE_VECTOR:
		{
			// table_vector
			dst_validate_table_vector(self, rel);

			// todo: subscription
			rel->cdc_sum   = 0;
			rel->cdc_count = 0;
			break;
		}
		case DST_REL_TOPIC:
		{
			// topic (subscription)
			auto ack = dst_validate_topic_sub(self, rel);
			if (ack)
			{
				dst_execute(self->dst, self->client,
				            "ACKNOWLEDGE topic_sub_{u64} TO {u64}, 10",
				             rel->id, ack);
				rel->cdc_sum   = 0;
				rel->cdc_count = 0;
			}
			break;
		}
		}
	}
}

void
dst_validate(Dst* self)
{
	for (auto i = 0; i < self->users_count; i++)
		dst_validate_user(&self->users[i]);

	info("[{u64}] OK", self->step);
}
