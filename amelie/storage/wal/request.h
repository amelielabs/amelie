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

typedef struct Request Request;

typedef enum
{
	META_UUID,
	META_TIMEZONE,
	META_TIME,
	META_SEED,
	META_REQUEST
} RequestMeta;

struct Request
{
	Str  rel;
	Str  content_type;
	Str  content;
	bool replay;
};

static inline void
request_init(Request* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
request_reset(Request* self)
{
	request_init(self);
}

static inline bool
request_empty(Request* self)
{
	return str_empty(&self->rel) && str_empty(&self->content);
}

static inline void
request_set_replay(Request* self)
{
	self->replay = true;
}

static inline void
request_write(Request* self, Endpoint* endpoint, Buf* buf, int flags)
{
	buf_reset(buf);
	write_init(buf, true);

	// []
	encode_array(buf);

	// uuid
	encode_int(buf, META_UUID);
	encode_uuid(buf, opt_uuid_of(&endpoint->db_id));

	// timezone
	encode_int(buf, META_TIMEZONE);
	encode_str(buf, opt_string_of(&endpoint->timezone));

	// time
	encode_int(buf, META_TIME);
	encode_int(buf, opt_int_of(&endpoint->time));

	// seed
	encode_int(buf, META_SEED);
	encode_int(buf, opt_int_of(&endpoint->seed));

	// request
	encode_int(buf, META_REQUEST);

	// []
	encode_array(buf);

	// rel
	encode_str(buf, &self->rel);

	// content_type
	encode_str(buf, &self->content_type);

	// content
	encode_str(buf, &self->content);
	encode_array_end(buf);

	encode_array_end(buf);

	// set write
	write_set(buf, MSG_RECORD);
	write_set_flags(write_of(buf), flags);
}

static inline void
request_read(Request* self, Endpoint* endpoint, Write* msg)
{
	auto record = msg->record;
	auto pos = record_data(record);
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		int64_t field;
		unpack_int(&pos, &field);

		int64_t integer;
		switch (field) {
		case META_UUID:
		{
			Str uuid;
			unpack_str(&pos, &uuid);
			uuid_set(&endpoint->db_id.uuid, &uuid);
			break;
		}
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
		case META_REQUEST:
		{
			unpack_array(&pos);
			unpack_str(&pos, &self->rel);
			unpack_str(&pos, &self->content_type);
			unpack_str(&pos, &self->content);
			unpack_array_end(&pos);
			break;
		}
		default:
			error("record: invalid data field");
			break;
		}
	}

	request_set_replay(self);
}
