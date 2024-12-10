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

typedef struct BufCache BufCache;
typedef struct Buf      Buf;

struct Buf
{
	uint8_t*   start;
	uint8_t*   position;
	uint8_t*   end;
	atomic_u32 refs;
	BufCache*  cache;
	List       link;
};

static inline void
buf_init(Buf* self)
{
	self->start    = NULL;
	self->position = NULL;
	self->end      = NULL;
	self->refs     = 0;
	self->cache    = NULL;
	list_init(&self->link);
}

static inline void
buf_free_memory(Buf* self)
{
	if (self->start)
		am_free(self->start);
	if (self->cache)
		am_free(self);
}

static inline void
buf_ref(Buf* self)
{
	atomic_u32_inc(&self->refs);
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

static inline bool
buf_empty(Buf* self)
{
	return self->position == self->start;
}

static inline void
buf_reset(Buf* self)
{
	self->position = self->start;
}

static inline uint8_t**
buf_reserve(Buf* self, int size)
{
	if (likely(buf_size_unused(self) >= size))
		return &self->position;

	int size_actual = buf_size(self) + size;
	int size_grow = buf_capacity(self) * 2;
	if (unlikely(size_actual > size_grow))
		size_grow = size_actual;

	uint8_t* pointer;
	pointer = am_realloc(self->start, size_grow);
	self->position = pointer + (self->position - self->start);
	self->end = pointer + size_grow;
	self->start = pointer;
	assert((self->end - self->position) >= size);
	return &self->position;
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

static inline void
buf_append_u8(Buf* self, uint8_t value)
{
	*self->position = value;
	buf_advance(self, 1);
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

static inline void*
buf_claim(Buf* self, int size)
{
	buf_reserve(self, size);
	auto pos = self->position;
	buf_advance(self, size);
	return pos;
}

static inline void
buf_write(Buf* self, void* data, int size)
{
	buf_reserve(self, size);
	buf_append(self, data, size);
}

static inline void
buf_write_buf(Buf* self, Buf* buf)
{
	buf_write(self, buf->start, buf_size(buf));
}

static inline void
buf_write_i8(Buf* self, int8_t value)
{
	buf_write(self, &value, sizeof(value));
}

static inline void
buf_write_i16(Buf* self, int16_t value)
{
	buf_write(self, &value, sizeof(value));
}

static inline void
buf_write_i32(Buf* self, int32_t value)
{
	buf_write(self, &value, sizeof(value));
}

static inline void
buf_write_i64(Buf* self, int64_t value)
{
	buf_write(self, &value, sizeof(value));
}

static inline void
buf_write_float(Buf* self, float value)
{
	buf_write(self, &value, sizeof(value));
}

static inline void
buf_write_double(Buf* self, double value)
{
	buf_write(self, &value, sizeof(value));
}

always_inline hot static inline void
buf_write_str(Buf* self, Str* str)
{
	buf_write(self, str_of(str), str_size(str));
}

static inline void
buf_vprintf(Buf* self, const char* fmt, va_list args)
{
	char tmp[512];
	int  tmp_len;
	tmp_len = vsnprintf(tmp, sizeof(tmp), fmt, args);
	buf_write(self, tmp, tmp_len);
}

static inline void
buf_printf(Buf* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	buf_vprintf(self, fmt, args);
	va_end(args);
}
