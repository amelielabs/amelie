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

typedef struct Str Str;

struct Str
{
	char* pos;
	char* end;
	bool  allocated;
};

static inline void
str_init(Str* self)
{
	self->pos = NULL;
	self->end = NULL;
	self->allocated = false;
}

static inline void
str_free(Str* self)
{
	if (self->allocated)
		am_free(self->pos);
	str_init(self);
}

static inline void
str_set_allocated(Str* self, char* pos, int size)
{
	self->pos = pos;
	self->end = pos + size;
	self->allocated = true;
}

static inline void
str_set(Str* self, char* pos, int size)
{
	self->pos = pos;
	self->end = pos + size;
	self->allocated = false;
}

static inline void
str_set_u8(Str* self, uint8_t* pos, int size)
{
	self->pos = (char*)pos;
	self->end = (char*)pos + size;
	self->allocated = false;
}

static inline void
str_set_cstr(Str* self, const char* cstr)
{
	str_set(self, (char*)cstr, strlen(cstr));
}

static inline void
str_set_str(Str* self, Str* str)
{
	self->pos = str->pos;
	self->end = str->end;
	self->allocated = false;
}

static inline int
str_size(Str* self)
{
	return self->end - self->pos;
}

static inline void
str_advance(Str *self, int size)
{
	self->pos += size;
	assert(self->pos <= self->end);
}

static inline void
str_truncate(Str *self, int size)
{
	self->end -= size;
	assert(self->pos <= self->end);
}

static inline bool
str_empty(Str* self)
{
	return str_size(self) == 0;
}

static inline char*
str_of(Str* self)
{
	return self->pos;
}

static inline uint8_t*
str_u8(Str* self)
{
	return (uint8_t*)self->pos;
}

static inline bool
str_is(Str* self, const void* string, int size)
{
	return str_size(self) == size && !memcmp(self->pos, string, size);
}

static inline bool
str_is_prefix(Str* self, const void* string, int size)
{
	return str_size(self) >= size && !memcmp(self->pos, string, size);
}

static inline bool
str_is_cstr(Str* self, const void* string)
{
	return str_is(self, string, strlen(string));
}

static inline bool
str_is_case(Str* self, const char* string, int size)
{
	return str_size(self) == size && !strncasecmp(self->pos, string, size);
}

static inline bool
str_compare(Str* self, Str* with)
{
	return str_is(self, str_of(with), str_size(with));
}

static inline bool
str_compare_prefix(Str* self, Str* prefix)
{
	return str_is_prefix(self, str_of(prefix), str_size(prefix));
}

hot static inline int
str_compare_fn(const Str* a, const Str* b)
{
	register int a_size = a->end - a->pos;
	register int b_size = b->end - b->pos;
	register int size;
	if (a_size < b_size)
		size = a_size;
	else
		size = b_size;
	auto rc = memcmp(a->pos, b->pos, size);
	if (rc != 0)
		return rc;
	return (a_size > b_size) - (a_size < b_size);
}

static inline char*
str_chr(Str* self, char token)
{
	for (auto pos = self->pos; pos < self->end; pos++)
		if (*pos == token)
			return pos;
	return NULL;
}

static inline bool
str_split(Str* self, Str* chunk, char token)
{
	str_init(chunk);
	char* pos = str_chr(self, token);
	if (pos == NULL)
	{
		str_set(chunk, str_of(self), str_size(self));
		return false;
	}
	str_set(chunk, str_of(self), pos - str_of(self));
	return true;
}

static inline void
str_arg(Str* self, Str* arg)
{
	str_init(arg);
	while (!str_empty(self) && isspace(*self->pos))
		str_advance(self, 1);
	if (str_empty(self))
		return;
	arg->pos = self->pos;
	while (!str_empty(self) && !isspace(*self->pos))
		str_advance(self, 1);
	arg->end = self->pos;
}

static inline void
str_shrink(Str* self)
{
	// remove whitespaces from start and end
	// of the string
	if (str_empty(self))
		return;
	while (self->pos < self->end && isspace(*self->pos))
		self->pos++;
	while ((self->end - 1) > self->pos && isspace(*(self->end - 1)))
		self->end--;
}

static inline void
str_chomp(Str* self)
{
	if (str_empty(self))
		return;
	if (self->end[-1] == '\n')
		self->end--;
}

static inline int
str_toint(Str* self, int64_t* value)
{
	char* pos = self->pos;
	char* end = self->end;
	*value = 0;
	while (pos < end)
	{
		if (unlikely(! isdigit(*pos)))
			return -1;
		if (unlikely(int64_mul_add_overflow(value, *value, 10, *pos - '0')))
			return -1;
		pos++;
	}
	return 0;
}

static inline void
str_dup(Str* self, const void* string, int size)
{
	char* pos = am_malloc(size + 1);
	memcpy(pos, string, size);
	pos[size] = 0;
	str_set_allocated(self, pos, size);
}

static inline void
str_dup_cstr(Str* self, const char* string)
{
	str_dup(self, string, strlen(string));
}

static inline void
str_copy(Str* self, Str* src)
{
	str_dup(self, str_of(src), str_size(src));
}
