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

static inline int
record_begin(Buf*     buf,
             Uuid*    id,
             Str*     timezone,
             uint64_t time,
             uint64_t seed,
             int      flags)
{
	auto offset = buf_size(buf);
	auto record = (Record*)buf_emplace(buf, sizeof(Record));
	record->crc   = 0;
	record->size  = 0;
	record->flags = flags;
	record->lsn   = 0;

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
	return offset;
}

static inline void
record_rel(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_REL);
		encode_str(buf, value);
	}
}

static inline void
record_content_type(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_CONTENT_TYPE);
		encode_str(buf, value);
	}
}

static inline void
record_content(Buf* buf, Str* value)
{
	if (value && !str_empty(value))
	{
		encode_int(buf, META_CONTENT);
		encode_str(buf, value);
	}
}

static inline void
record_end(Buf* buf, int offset)
{
	// request end
	encode_array_end(buf);

	// set size
	auto record = (Record*)(buf->start + offset);
	record->size = buf_size(buf) - offset;
}

static inline void
record_read(uint8_t*  pos,
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
