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

typedef struct Readahead Readahead;

struct Readahead
{
	int  offset;
	int  readahead;
	Tcp* tcp;
	Buf  buf;
};

static inline void
readahead_init(Readahead* self, Tcp* tcp, int size)
{
	self->offset    = 0;
	self->readahead = size;
	self->tcp       = tcp;
	buf_init(&self->buf);
}

static inline void
readahead_free(Readahead* self)
{
	buf_free(&self->buf);
}

static inline void
readahead_reset(Readahead* self)
{
	self->offset = 0;
	buf_reset(&self->buf);
}

hot static inline bool
readahead_pending(Readahead* self)
{
	return (buf_size(&self->buf) - self->offset) > 0;
}

hot static inline int
readahead_read(Readahead* self, int size, uint8_t** pos)
{
	for (;;)
	{
		int left = buf_size(&self->buf) - self->offset;
		if (likely(left > 0))
		{
			if (left < size)
				size = left;
			*pos = self->buf.start + self->offset;
			self->offset += size;
			break;
		}
		self->offset = 0;
		buf_reset(&self->buf);
		int rc = tcp_read(self->tcp, &self->buf, self->readahead);
		if (unlikely(rc == 0))
			return 0;
	}
	return size;
}

hot static inline bool
readahead_recv(Readahead* self, uint8_t* data, int size)
{
	for (auto at = 0; at < size; )
	{
		uint8_t* pos;
		auto rc = readahead_read(self, size, &pos);
		if (rc == 0)
			return false;
		memcpy(data + at, pos, rc);
		at += rc;
		size -= rc;
	}
	return true;
}

hot static inline bool
readahead_recv_buf(Readahead* self, Buf* buf, int size)
{
	buf_reserve(buf, size);
	if (! readahead_recv(self, buf->position, size))
		return false;
	buf_advance(buf, size);
	return true;
}

static inline void
readahead_pushback(Readahead* self, int size)
{
	self->offset -= size;
	assert(self->offset >= 0);
}
