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

enum
{
	META_UUID,
	META_TIMEZONE,
	META_TIME,
	META_SEED,
	META_REL,
	META_CONTENT_TYPE,
	META_CONTENT
};

static inline void
write_begin_at(Buf*     buf,
               Uuid*    id,
               Str*     timezone,
               uint64_t time,
               uint64_t seed)
{
	write_init(buf, true);

	// []
	encode_array(buf);

	// uuid
	encode_int(buf, META_UUID);
	encode_uuid(buf, id);

	// timezone
	encode_int(buf, META_TIMEZONE);
	encode_str(buf, timezone);

	// time
	encode_int(buf, META_TIME);
	encode_int(buf, time);

	// seed
	encode_int(buf, META_SEED);
	encode_int(buf, seed);

	// (incomplete)
}

static inline Buf*
write_begin(Uuid*    id,
            Str*     timezone,
            uint64_t time,
            uint64_t seed)
{
	auto buf = buf_create();
	write_begin_at(buf, id, timezone, time, seed);
	return buf;
}

static inline void
write_rel(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_REL);
		encode_str(buf, value);
	}
}

static inline void
write_content_type(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_CONTENT_TYPE);
		encode_str(buf, value);
	}
}

static inline void
write_content(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_CONTENT);
		encode_str(buf, value);
	}
}

static inline void
write_end(Buf* buf, MsgId id, uint64_t ack, uint8_t flags)
{
	// request end
	encode_array_end(buf);

	write_set(buf, id);

	auto record = write_of(buf)->record;
	record->crc   = 0;
	record->size  = buf_size(buf) - sizeof(Write);
	record->flags = flags;
	record->lsn   = 0;
	record->ack   = ack;
}

static inline void
write_read(uint8_t*  pos,
           Uuid*     id,
           Str*      timezone,
           uint64_t* time,
           uint64_t* seed,
           Str*      rel,
           Str*      content_type,
           Str*      content)
{
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		int64_t field;
		unpack_int(&pos, &field);

		switch (field) {
		case META_UUID:
		{
			Str uuid;
			unpack_str(&pos, &uuid);
			uuid_set(id, &uuid);
			break;
		}
		case META_TIMEZONE:
			unpack_str(&pos, timezone);
			break;
		case META_TIME:
		{
			int64_t i64;
			unpack_int(&pos, &i64);
			*time = (uint64_t)i64;
			break;
		}
		case META_SEED:
		{
			int64_t i64;
			unpack_int(&pos, &i64);
			*seed = (uint64_t)i64;
			break;
		}
		case META_REL:
			unpack_str(&pos, rel);
			break;
		case META_CONTENT_TYPE:
			unpack_str(&pos, content_type);
			break;
		case META_CONTENT:
			unpack_str(&pos, content);
			break;
		default:
			error("record: invalid data field");
			break;
		}
	}
}
