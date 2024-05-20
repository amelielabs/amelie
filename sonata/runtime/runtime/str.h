#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
		so_free(self->pos);
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
str_strncasecmp(Str* self, const char* string, int size)
{
	return str_size(self) == size && !strncasecmp(self->pos, string, size);
}

static inline bool
str_compare_raw(Str* self, const void* string, int size)
{
	return str_size(self) == size && !memcmp(self->pos, string, size);
}

static inline bool
str_compare_raw_prefix(Str* self, const void* string, int size)
{
	return str_size(self) >= size && !memcmp(self->pos, string, size);
}

static inline bool
str_compare_cstr(Str* self, const void* string)
{
	return str_compare_raw(self, string, strlen(string));
}

static inline bool
str_compare(Str* self, Str* with)
{
	return str_compare_raw(self, str_of(with), str_size(with));
}

static inline bool
str_compare_prefix(Str* self, Str* prefix)
{
	return str_compare_raw_prefix(self, str_of(prefix), str_size(prefix));
}

static inline int
str_compare_fn(Str* a, Str* b)
{
	register int a_size = str_size(a);
	register int b_size = str_size(b);
	register int size;
	if (a_size < b_size)
		size = a_size;
	else
		size = b_size;

	int rc;
	rc = memcmp(a->pos, b->pos, size);
	if (rc == 0) {
		if (likely(a_size == b_size))
			return 0;
		return (a_size < b_size) ? -1 : 1;
	}

	return rc > 0 ? 1 : -1;
}

static inline bool
str_split_or_set(Str* self, Str* chunk, char token)
{
	str_init(chunk);
	char* pos = strnchr(str_of(self), str_size(self), token);
	if (pos == NULL)
	{
		str_set(chunk, str_of(self), str_size(self));
		return false;
	}
	str_set(chunk, str_of(self), pos - str_of(self));
	return true;
}

static inline void
str_shrink(Str* self)
{
	// remove whitespaces from start and end
	// of the string
	while (self->pos < self->end && isspace(*self->pos))
		self->pos++;
	while ((self->end - 1) > self->pos && isspace(*(self->end - 1)))
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
		*value = (*value * 10) + *pos - '0';
		pos++;
	}
	return 0;
}
