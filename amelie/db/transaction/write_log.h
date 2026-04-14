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

static inline bool
write_log_empty(WriteLog* self)
{
	return buf_empty(&self->meta);
}

hot static inline void
write_log_add(WriteLog* self, int cmd, Uuid* id, uint8_t* data, int data_size)
{
	// prepare and reuse last command header
	RecordCmd* hdr = NULL;
	if (likely(! buf_empty(&self->meta)))
	{
		hdr = (RecordCmd*)(self->meta.position - sizeof(*hdr));
		if (hdr->cmd != cmd || !uuid_is(&hdr->id, id))
			hdr = NULL;
	}
	if (unlikely(! hdr))
	{
		hdr = buf_emplace(&self->meta, sizeof(*hdr));
		hdr->cmd  = cmd;
		hdr->size = 0;
		hdr->crc  = 0;
		hdr->id   = *id;
	}

	// add data
	iov_add(&self->iov, data, data_size);
	hdr->size += data_size;

	// calculate crc
	if (opt_int_of(&config()->wal_crc))
		hdr->crc = runtime()->crc(hdr->crc, data, data_size);
}

hot static inline void
write_log_add_cmd(WriteLog* self, int cmd, Uuid* id, uint8_t* data, int data_size)
{
	// prepare command header
	auto hdr = (RecordCmd*)buf_emplace(&self->meta, sizeof(RecordCmd));
	hdr->cmd  = cmd;
	hdr->size = data_size;
	hdr->crc  = 0;
	if (id)
		hdr->id = *id;
	else
		uuid_init(&hdr->id);

	// op
	iov_add(&self->iov, data, data_size);

	// calculate crc
	if (opt_int_of(&config()->wal_crc))
		hdr->crc = runtime()->crc(0, data, data_size);
}
