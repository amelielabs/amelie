#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct IovChunk IovChunk;
typedef struct Iov      Iov;

struct IovChunk
{
	bool is_pointer;
	union {
		struct {
			Buf* buf;
			int  offset;
			int  size;
		} buf;
		struct {
			void* pointer;
			int   size;
		} pointer;
	};
};

struct Iov
{
	int count;
	Buf chunks;
	Buf iov;
};

static inline void
iov_init(Iov* self)
{
	self->count = 0;
	buf_init(&self->chunks);
	buf_init(&self->iov);
}

static inline void
iov_free(Iov* self)
{
	buf_free_memory(&self->chunks);
	buf_free_memory(&self->iov);
}

static inline void
iov_reset(Iov* self)
{
	self->count = 0;
	buf_reset(&self->chunks);
	buf_reset(&self->iov);
}

static inline struct iovec*
iov_pointer(Iov* self)
{
	return (struct iovec*)self->iov.start;
}

static inline void
iov_add_buf(Iov* self, Buf* buf, int offset, int size)
{
	buf_reserve(&self->chunks, sizeof(IovChunk));
	auto chunk = (IovChunk*)self->chunks.position;
	chunk->is_pointer = false;
	chunk->buf.buf    = buf;
	chunk->buf.offset = offset;
	chunk->buf.size   = size;
	buf_advance(&self->chunks, sizeof(IovChunk));
	self->count++;
}

static inline void
iov_add(Iov* self, void* pointer, int size)
{
	buf_reserve(&self->chunks, sizeof(IovChunk));
	auto chunk = (IovChunk*)self->chunks.position;
	chunk->is_pointer = true;
	chunk->pointer.pointer = pointer;
	chunk->pointer.size    = size;
	buf_advance(&self->chunks, sizeof(IovChunk));
	self->count++;
}

hot static inline void
iov_prepare(Iov* self)
{
	int size_iov = sizeof(struct iovec) * self->count;
	buf_reserve(&self->iov, size_iov);
	buf_advance(&self->iov, size_iov);

	auto iovec = (struct iovec*)self->iov.start;
	auto chunk = (IovChunk*)self->chunks.start;
	auto end   = (IovChunk*)self->chunks.position;
	while (chunk < end)
	{
		if (chunk->is_pointer)
		{
			iovec->iov_base = chunk->pointer.pointer;
			iovec->iov_len  = chunk->pointer.size;
		} else
		{
			iovec->iov_base = chunk->buf.buf->start + chunk->buf.offset;
			iovec->iov_len  = chunk->buf.size;
		}
		chunk++;
		iovec++;
	}
}

hot static inline void
iov_import(Iov* self, Iov* from)
{
	int size_iov = sizeof(struct iovec) * from->count;
	buf_reserve(&self->iov, size_iov);
	buf_advance(&self->iov, size_iov);

	auto iovec = (struct iovec*)self->iov.start;
	auto chunk = (IovChunk*)from->chunks.start;
	auto end   = (IovChunk*)from->chunks.position;
	while (chunk < end)
	{
		if (chunk->is_pointer)
		{
			iovec->iov_base = chunk->pointer.pointer;
			iovec->iov_len  = chunk->pointer.size;
		} else
		{
			iovec->iov_base = chunk->buf.buf->start + chunk->buf.offset;
			iovec->iov_len  = chunk->buf.size;
		}
		chunk++;
		iovec++;
	}
}

hot static inline void
iov_copy(Buf* buf, struct iovec* iov, int count)
{
	buf_reset(buf);
	while (count > 0)
	{
		buf_write(buf, iov->iov_base, iov->iov_len);
		iov++;
		count--;
	}
}
