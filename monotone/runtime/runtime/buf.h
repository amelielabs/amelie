#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct BufCache BufCache;
typedef struct BufPool  BufPool;
typedef struct Buf      Buf;

struct Buf
{
	uint8_t*  start;
	uint8_t*  position;
	uint8_t*  end;
	int       refs;
	BufPool*  pool;
	BufCache* cache;
	List      link;
};

static inline void
buf_init(Buf* self)
{
	self->start    = NULL;
	self->position = NULL;
	self->end      = NULL;
	self->refs     = 0;
	self->pool     = NULL;
	self->cache    = NULL;
	list_init(&self->link);
}

static inline void
buf_free_memory(Buf* self)
{
	if (self->start)
		mn_free(self->start);
	if (self->cache)
		mn_free(self);
}

static inline int
buf_capacity(Buf* self)
{
	return self->end - self->start;
}

static inline int
buf_size_unused(Buf* self)
{
	return self->end - self->position;
}

static inline int
buf_size(Buf* self)
{
	return self->position - self->start;
}

static inline void
buf_reset(Buf* self)
{
	self->position = self->start;
}

static inline int
buf_reserve_nothrow(Buf* self, int size)
{
	if (likely(buf_size_unused(self) >= size))
		return 0;

	int size_actual = buf_size(self) + size;
	int size_grow = buf_capacity(self)*  2;
	if (unlikely(size_actual > size_grow))
		size_grow = size_actual;

	uint8_t* pointer;
	pointer = mn_realloc_nothrow(self->start, size_grow);
	if (unlikely(pointer == NULL))
		return -1;

	self->position = pointer + (self->position - self->start);
	self->end = pointer + size_grow;
	self->start = pointer;
	assert((self->end - self->position) >= size);
	return 0;
}

static inline void
buf_advance(Buf* self, int size)
{
	self->position += size;
	assert(self->position <= self->end);
}

static inline void
buf_truncate(Buf* self, int size)
{
	self->position -= size;
	assert(self->position >= self->start);
}

static inline void
buf_append(Buf* self, void* ptr, int size)
{
	memcpy(self->position, ptr, size);
	buf_advance(self, size);
}

static inline char*
buf_cstr(Buf* self)
{
	return (char*)self->start;
}

static inline uint32_t*
buf_u32(Buf* self)
{
	return (uint32_t*)self->start;
}

static inline uint64_t*
buf_u64(Buf* self)
{
	return (uint64_t*)self->start;
}

static inline void
buf_str(Buf* self, Str* str)
{
	str_set(str, (char*)self->start, buf_size(self));
}
