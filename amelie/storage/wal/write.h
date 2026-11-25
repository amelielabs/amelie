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

typedef struct Write Write;

struct Write
{
	Msg    msg;
	Buf*   msg_buf;
	void*  arg;
	Record record[];
};

always_inline static inline Write*
write_of(Buf* buf)
{
	return (Write*)buf->start;
}

static inline void
write_init(Buf* buf, bool reserve_record)
{
	auto size = sizeof(Write);
	if (reserve_record)
		size += sizeof(Record);
	buf_emplace(buf, size);
}

static inline void
write_set(Buf* buf, MsgId id)
{
	auto msg = (Write*)buf->start;
	msg_init(&msg->msg, id);
	msg->msg_buf     = buf;
	msg->arg         = NULL;
}

static inline Write*
write_copy(MsgId id, Record* record)
{
	auto self = buf_create();
	write_init(self, false);
	buf_reserve(self, record->size);
	buf_write(self, record, record->size);
	write_set(self, id);
	return write_of(self);
}

static inline void
write_seal(Write* self, uint64_t lsn)
{
	auto record = &self->record[0];
	record->lsn = lsn;

	// calculate crc
	if (config()->wal_crc)
		record->crc = runtime()->crc(0, &record->size, record->size - sizeof(uint32_t));
}
