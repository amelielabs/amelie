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

static inline Buf*
write_allocate(bool reserve_record)
{
	auto buf = buf_create();
	write_init(buf, reserve_record);
	return buf;
}

static inline void
write_complete_msg(Buf* buf, MsgId id)
{
	auto msg = (Write*)buf->start;
	msg_init(&msg->msg, id);
	msg->msg_buf     = buf;
	msg->arg         = NULL;
}

static inline void
write_complete(Buf* buf, MsgId id, uint64_t ack, uint8_t flags)
{
	write_complete_msg(buf, id);

	auto record = write_of(buf)->record;
	record->crc   = 0;
	record->size  = buf_size(buf) - sizeof(Write);
	record->flags = flags;
	record->lsn   = 0;
	record->ack   = ack;
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

static inline Write*
write_copy(MsgId id, Record* record)
{
	auto self = write_allocate(false);
	buf_reserve(self, record->size);
	buf_write(self, record, record->size);

	write_complete_msg(self, id);
	return write_of(self);
}
