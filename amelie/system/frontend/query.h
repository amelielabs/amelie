#pragma once

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

typedef struct Query Query;

typedef enum
{
	QUERY_UNDEF,
	QUERY_SQL,
	QUERY_WRITE,
	QUERY_EXECUTE
} QueryType;

typedef enum
{
	META_TIMEZONE,
	META_TIME,
	META_SEED,
	META_USER,
	META_SQL,
	META_WRITE
} QueryMeta;

struct Query
{
	QueryType type;
	Record*   recover;
	uint64_t  recover_id;
	union
	{
		Str text;
		struct {
			Str       rel_user;
			Str       rel;
			uint8_t*  args;
			int       args_size;
		};
	};
};

static inline void
query_init(Query* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
query_reset(Query* self)
{
	query_init(self);
}

static inline void
query_write(Query* self, Endpoint* endpoint, Buf* buf)
{
	// []
	encode_array(buf);

	// timezone
	encode_int(buf, META_TIMEZONE);
	encode_str(buf, opt_string_of(&endpoint->timezone));

	// time
	encode_int(buf, META_TIME);
	encode_int(buf, opt_int_of(&endpoint->time));

	// seed
	encode_int(buf, META_SEED);
	encode_int(buf, opt_int_of(&endpoint->seed));

	// user
	encode_int(buf, META_USER);
	encode_str(buf, opt_string_of(&endpoint->user));

	// data
	switch (self->type) {
	case QUERY_SQL:
	{
		encode_int(buf, META_SQL);
		encode_str(buf, &self->text);
		break;
	}
	case QUERY_WRITE:
	case QUERY_EXECUTE:
	{
		encode_int(buf, META_WRITE);

		// []
		encode_array(buf);

		// rel_user
		encode_str(buf, &self->rel_user);

		// rel
		encode_str(buf, &self->rel);

		// args
		buf_write(buf, self->args, self->args_size);

		encode_array_end(buf);
		break;
	}
	default:
		abort();
	}

	encode_array_end(buf);
}

static inline void
query_read(Query* self, Endpoint* endpoint, Record* record, uint64_t record_id)
{
	auto pos = record_data(record);
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		int64_t field;
		unpack_int(&pos, &field);

		int64_t integer;
		switch (field) {
		case META_TIMEZONE:
			unpack_str(&pos, &endpoint->timezone.string);
			break;
		case META_TIME:
			unpack_int(&pos, &integer);
			opt_int_set(&endpoint->time, integer);
			break;
		case META_SEED:
			unpack_int(&pos, &integer);
			opt_int_set(&endpoint->seed, integer);
			break;
		case META_USER:
			unpack_str(&pos, &endpoint->user.string);
			break;
		case META_SQL:
		{
			self->type = QUERY_SQL;
			unpack_str(&pos, &self->text);
			break;
		}
		case META_WRITE:
		{
			self->type = QUERY_WRITE;
			unpack_array(&pos);
			unpack_str(&pos, &self->rel_user);
			unpack_str(&pos, &self->rel);
			self->args      = pos;
			data_skip(&pos);
			self->args_size = pos - self->args;
			unpack_array_end(&pos);
			break;
		}
		default:
			error("record: invalid data field");
			break;
		}
	}

	self->recover    = record;
	self->recover_id = record_id;
}
