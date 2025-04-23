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

typedef struct WriteLog WriteLog;

struct WriteLog
{
	Buf  meta;
	Iov  iov;
	List link;
};

static inline void
write_log_init(WriteLog* self)
{
	buf_init(&self->meta);
	iov_init(&self->iov);
	list_init(&self->link);
}

static inline void
write_log_free(WriteLog* self)
{
	buf_free(&self->meta);
	iov_free(&self->iov);
}

static inline void
write_log_reset(WriteLog* self)
{
	buf_reset(&self->meta);
	iov_reset(&self->iov);
	list_init(&self->link);
}

hot static inline void
write_log_add(WriteLog* self, int cmd, uint64_t partition, Row* row)
{
	// prepare and reuse last command header
	RecordCmd* hdr = NULL;
	if (likely(! buf_empty(&self->meta)))
	{
		hdr = (RecordCmd*)(self->meta.position - sizeof(*hdr));
		if (hdr->cmd != cmd || hdr->partition != partition)
			hdr = NULL;
	}
	if (unlikely(! hdr))
	{
		hdr = buf_claim(&self->meta, sizeof(*hdr));
		hdr->cmd       = cmd;
		hdr->size      = 0;
		hdr->crc       = 0;
		hdr->partition = partition;
	}

	// add row
	iov_add(&self->iov, row, row_size(row));
	hdr->size += row_size(row);

	// calculate crc
	if (opt_int_of(&config()->wal_crc))
		hdr->crc = global()->crc(hdr->crc, row, row_size(row));
}

hot static inline void
write_log_add_op(WriteLog* self, int cmd, Buf* op)
{
	// prepare command header
	auto hdr = (RecordCmd*)buf_claim(&self->meta, sizeof(RecordCmd));
	hdr->cmd       = cmd;
	hdr->size      = buf_size(op);
	hdr->crc       = 0;
	hdr->partition = 0;

	// op
	iov_add_buf(&self->iov, op);

	// calculate crc
	if (opt_int_of(&config()->wal_crc))
		hdr->crc = global()->crc(0, op->start, buf_size(op));
}
