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
	REQUEST_UNDEF,
	REQUEST_SQL,
	REQUEST_WRITE,
	REQUEST_EXECUTE
} RequestType;

typedef enum
{
	META_USER,
	META_TIMEZONE,
	META_TIME,
	META_SEED,
	META_SQL,
	META_WRITE
} RequestMeta;

struct Request
{
	RequestType type;
	RecordMsg*  recover;
	union
	{
		Str text;
		struct {
			Str      rel_user;
			Str      rel;
			uint8_t* args;
			int      args_size;
		};
	};
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

static inline void
request_write(Request* self, Endpoint* endpoint, Buf* buf)
{
	// []
	encode_array(buf);

	// user
	encode_int(buf, META_USER);
	encode_str(buf, opt_string_of(&endpoint->user));

	// timezone
	encode_int(buf, META_TIMEZONE);
	encode_str(buf, opt_string_of(&endpoint->timezone));

	// time
	encode_int(buf, META_TIME);
	encode_int(buf, opt_int_of(&endpoint->time));

	// seed
	encode_int(buf, META_SEED);
	encode_int(buf, opt_int_of(&endpoint->seed));

	// data
	switch (self->type) {
	case REQUEST_SQL:
	{
		encode_int(buf, META_SQL);
		encode_str(buf, &self->text);
		break;
	}
	case REQUEST_WRITE:
	case REQUEST_EXECUTE:
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
request_read(Request* self, Endpoint* endpoint, RecordMsg* msg)
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
		case META_USER:
			unpack_str(&pos, &endpoint->user.string);
			break;
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
		case META_SQL:
		{
			self->type = REQUEST_SQL;
			unpack_str(&pos, &self->text);
			break;
		}
		case META_WRITE:
		{
			self->type = REQUEST_WRITE;
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

	self->recover = msg;
}
