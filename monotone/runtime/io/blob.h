#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Blob Blob;

struct Blob
{
	uint8_t* start;
	uint8_t* position;
	uint8_t* end;
	size_t   step;
};

always_inline static inline size_t
blob_size_allocated(Blob* self)
{
	return self->end - self->start;
}

always_inline static inline size_t
blob_size(Blob* self)
{
	return self->position - self->start;
}

always_inline static inline size_t
blob_capacity(Blob* self)
{
	return self->end - self->position;
}

static inline void
blob_init(Blob* self, size_t step)
{
	self->start    = NULL;
	self->position = NULL;
	self->end      = NULL;
	self->step     = step;
}

static inline void
blob_free(Blob* self)
{
	if (self->start)
	{
		vfs_munmap(self->start, blob_size_allocated(self));
		self->start = NULL;
	}
}

static inline uint8_t**
blob_reserve(Blob* self, size_t size)
{
	if (likely(blob_capacity(self) >= size))
		return &self->position;

	size_t size_required = blob_size(self) + size;
	size_t size_next     = blob_size_allocated(self) + self->step;
	if (unlikely(size_required > size_next))
		size_next = size_required;

	uint8_t* pointer;
	if (self->start == NULL)
		pointer = vfs_mmap(-1, size_next);
	else
		pointer = vfs_mremap(self->start, blob_size_allocated(self), size_next);
	if (unlikely(pointer == NULL))
		error_system();

	self->position = pointer + (self->position - self->start);
	self->end      = pointer + size_next;
	self->start    = pointer;
	assert(blob_capacity(self) >= size);
	return &self->position;
}

static inline uint8_t*
blob_data(Blob* self)
{
	return self->start;
}

static inline uint8_t*
blob_pos(Blob* self)
{
	return self->position;
}

static inline void
blob_reset(Blob* self)
{
	self->position = self->start;
}

static inline void
blob_advance(Blob* self, size_t size)
{
	self->position += size;
	assert(self->position <= self->end);
}

static inline void
blob_write(Blob* self, void* data, size_t size)
{
	blob_reserve(self, size);
	memcpy(self->position, data, size);
	blob_advance(self, size);
}

static inline void
blob_writev(Blob* self, struct iovec* iov, int iov_count)
{
	int i = 0;
	for (; i < iov_count; i++)
		blob_write(self, iov[i].iov_base, iov[i].iov_len);
}
