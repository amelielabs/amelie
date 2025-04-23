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
	Record header;
	Iov    iov;
	int    list_count;
	List   list;
	List   link;
};

static inline void
write_init(Write* self)
{
	memset(&self->header, 0, sizeof(self->header));
	self->list_count = 0;
	list_init(&self->list);
	iov_init(&self->iov);
	list_init(&self->link);
}

static inline void
write_free(Write* self)
{
	iov_free(&self->iov);
}

static inline void
write_reset(Write* self)
{
	memset(&self->header, 0, sizeof(self->header));
	self->list_count = 0;
	list_init(&self->list);
	iov_reset(&self->iov);
}

static inline void
write_begin(Write* self)
{
	auto header = &self->header;
	header->crc   = 0;
	header->lsn   = 0;
	header->size  = sizeof(self->header);
	header->count = 0;
	header->ops   = 0;
	iov_add(&self->iov, header, sizeof(self->header));
}

static inline void
write_end(Write* self, uint64_t lsn)
{
	auto header = &self->header;
	header->lsn = lsn;

	// calculate header crc (header + commands)
	if (opt_int_of(&config()->wal_crc))
		header->crc = global()->crc(header->crc, &header->lsn,
		                            sizeof(Record) - sizeof(uint32_t));
}

static inline void
write_add(Write* self, WriteLog* write_log)
{
	// [header][commands][rows or ops]
	list_append(&self->list, &write_log->link);
	self->list_count++;

	// append commands
	iov_add_buf(&self->iov, &write_log->meta);

	auto header = &self->header;
	header->size  += buf_size(&write_log->meta) + write_log->iov.size;
	header->count += buf_size(&write_log->meta) / sizeof(RecordCmd);
	header->ops   += write_log->iov.iov_count;

	// calculate crc
	if (opt_int_of(&config()->wal_crc))
		header->crc = global()->crc(header->crc,
		                            write_log->meta.start,
		                            buf_size(&write_log->meta));
}
