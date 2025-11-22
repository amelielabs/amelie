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

typedef struct SltHashKey SltHashKey;
typedef struct SltHash    SltHash;

struct SltHashKey
{
	int  label_size;
	char label[26];
	char digest[33];
} packed;

struct SltHash
{
	SltHashKey* hash;
	int         hash_size;
	int         count;
};

static inline void
slt_hash_init(SltHash* self)
{
	self->hash      = NULL;
	self->hash_size = 0;
	self->count     = 0;
}

static inline void
slt_hash_free(SltHash* self)
{
	if (self->hash)
		am_free(self->hash);
}

static inline void
slt_hash_reset(SltHash* self)
{
	if (self->hash)
		memset(self->hash, 0, sizeof(SltHashKey) * self->hash_size);
}

static inline void
slt_hash_create(SltHash* self, int size)
{
	auto to_allocate = sizeof(SltHashKey) * size;
	self->hash_size = size;
	self->hash = am_malloc(to_allocate);
	memset(self->hash, 0, to_allocate);
}

static inline SltHashKey*
slt_hash_get_or_set(SltHash* self, char* label, int label_size)
{
	uint32_t hash = hash_fnv(label, label_size);
	int pos = hash % self->hash_size;
	for (;;)
	{
		auto ref = &self->hash[pos];
		if (! ref->label_size)
			return ref;
		if (ref->label_size == label_size)
			if (! memcmp(label, ref->label, ref->label_size))
				return ref;
		pos = (pos + 1) % self->hash_size;
	}
}

static inline void
slt_hash_key_set(SltHashKey* self, char* label, int label_size, char digest[33])
{
	assert(label_size <= (int)sizeof(self->label));
	self->label_size = label_size;
	memcpy(self->label, label, label_size);
	memcpy(self->digest, digest, sizeof(self->digest));
}

static inline void
slt_hash_resize(SltHash* self)
{
	SltHash hash;
	slt_hash_init(&hash);
	slt_hash_create(&hash, self->hash_size * 2);
	for (auto at = 0; at < self->hash_size; at++)
	{
		auto pos = &self->hash[at];
		if (! pos->label_size)
			continue;
		auto ref = slt_hash_get_or_set(&hash, pos->label, pos->label_size);
		assert(! ref->label_size);
		slt_hash_key_set(ref, pos->label, pos->label_size, pos->digest);
		hash.count++;
	}
	slt_hash_free(self);
	*self = hash;
}

static inline bool
slt_hash_add(SltHash* self, Str* label, Buf* digest)
{
	if (unlikely(! self->hash))
		slt_hash_create(self, 256);

	if (unlikely(self->count >= (self->hash_size / 2)))
		slt_hash_resize(self);

	// find or create new record
	auto ref = slt_hash_get_or_set(self, str_of(label), str_size(label));
	if (! ref->label_size)
	{
		slt_hash_key_set(ref, str_of(label), str_size(label), buf_cstr(digest));
		self->count++;
		return true;
	}

	// record exists, validate digest
	return !memcmp(ref->digest, digest->start, sizeof(ref->digest));
}
