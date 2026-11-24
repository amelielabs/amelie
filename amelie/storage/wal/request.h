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
	META_REL,
	META_CONTENT_TYPE,
	META_CONTENT
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
request_begin(Buf* buf, Endpoint* endpoint, Uuid* id, Str* rel, Str* content_type)
{
	buf_reset(buf);
	write_init(buf, true);

	// []
	encode_array(buf);

	// uuid
	encode_int(buf, META_UUID);
	encode_uuid(buf, id);

	// timezone
	encode_int(buf, META_TIMEZONE);
	encode_str(buf, opt_string_of(&endpoint->timezone));

	// time
	encode_int(buf, META_TIME);
	encode_int(buf, opt_int_of(&endpoint->time));

	// seed
	encode_int(buf, META_SEED);
	encode_int(buf, opt_int_of(&endpoint->seed));

	// rel
	if (rel && !str_empty(rel))
	{
		encode_int(buf, META_REL);
		encode_str(buf, rel);
	}

	// content_type
	if (! str_empty(content_type))
	{
		encode_int(buf, META_CONTENT_TYPE);
		encode_str(buf, content_type);
	}

	// (incomplete)
}

static inline void
request_end(Buf* buf, MsgId id, uint64_t ack, int flags)
{
	// request end
	encode_array_end(buf);

	// write
	write_complete(buf, id, ack, flags);
}

static inline void
request_write(Request* self, Endpoint* endpoint, Buf* buf, uint64_t ack, int flags)
{
	request_begin(buf, endpoint, opt_uuid_of(&endpoint->db_id),
	              &self->rel, &self->content_type);

	// content
	auto content = &self->content;
	if (! str_empty(content))
	{
		encode_int(buf, META_CONTENT);
		encode_str(buf, content);
	}

	request_end(buf, MSG_RECORD, ack, flags);
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
		case META_REL:
		{
			unpack_str(&pos, &self->rel);
			break;
		}
		case META_CONTENT_TYPE:
		{
			unpack_str(&pos, &self->content_type);
			break;
		}
		case META_CONTENT:
		{
			unpack_str(&pos, &self->content);
			break;
		}
		default:
			error("record: invalid data field");
			break;
		}
	}

	if (msg->msg.id == MSG_RECORD)
		request_set_replay(self);
}
