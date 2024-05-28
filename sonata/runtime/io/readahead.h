#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Readahead Readahead;

struct Readahead
{
	int offset;
	int readahead;
	Buf buf;
};

static inline void
readahead_init(Readahead* self, int size)
{
	self->offset    = 0;
	self->readahead = size;
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

hot static inline int
readahead_read(Readahead* self, Tcp* tcp, int size, uint8_t** pos)
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
		int rc = tcp_read(tcp, &self->buf, self->readahead);
		if (unlikely(rc == 0))
			error("disconnected");
	}
	return size;
}

static inline void
readahead_pushback(Readahead* self, int size)
{
	self->offset -= size;
	assert(self->offset >= 0);
}
