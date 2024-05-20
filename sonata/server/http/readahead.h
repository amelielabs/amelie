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
readahead_init(Readahead* self)
{
	self->offset    = 0;
	self->readahead = 16 * 1024;
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
readahead_header_eof(Readahead* self)
{
	auto start = self->buf.start + self->offset;
	auto end   = self->buf.position;
	uint8_t* pos = memmem(start, end - start, "\r\n\r\n", 4);
	if (pos)
		return self->offset + ((pos + 4) - start);
	return -1;
}

hot static inline void
readahead_read(Readahead* self, Tcp* tcp, int size)
{
	int rc = tcp_read(tcp, &self->buf, size);
	if (rc > 0)
	{
		// reserve 1 byte for \0
		buf_reserve(&self->buf, 1);
		*self->buf.position = '\0';
	} else {
		if (tcp->eof)
			error("disconnected");
	}
}

hot static inline void
readahead_header(Readahead* self, Tcp* tcp, Str* header)
{
	int start = self->offset;
	for (;;)
	{
		// match header eof marker
		int eof = readahead_header_eof(self);
		if (likely(eof >= 0)) {
			self->offset = eof;
			break;
		}

		// header is incomplete, need more data
		int leftover = buf_size(&self->buf) - start;
		if (leftover > 0)
		{
			// todo: validate max header size
			memmove(self->buf.start, self->buf.start + start, leftover);
			self->buf.position = self->buf.start + leftover;
		}
		self->offset = 0;
		start = 0;

		// fill readahead buffer
		readahead_read(self, tcp, self->readahead);	
	}

	str_set(header, (char*)self->buf.start + start, self->offset - start);
}

hot static inline void
readahead_content(Readahead* self, Tcp* tcp, Str* content, int size)
{
	int start = self->offset;
	for (;;)
	{
		int leftover = buf_size(&self->buf) - start;
		if (likely(leftover >= size))
		{
			self->offset += size;
			break;
		}

		// content is incomplete, need more data
		if (start > 0 && leftover > 0)
		{
			memmove(self->buf.start, self->buf.start + start, leftover);
			self->buf.position = self->buf.start + leftover;
		}
		self->offset = 0;
		start = 0;

		// fill readahead buffer
		int readahead = self->readahead;
		if (size > readahead)
			readahead = size;
		readahead_read(self, tcp, readahead);	
	}

	str_set(content, (char*)self->buf.start + start, self->offset - start);
}
