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

static inline uint32_t
hash_fnv(const char* data, int size)
{
	auto pos = (const unsigned char*)data;
	auto end = (const unsigned char*)pos + size;
	uint32_t hash = 2166136261;
	while (pos < end)
	{
		hash = (hash * 16777619) ^ *pos;
		pos++;
	}
	return hash;
}

static inline uint32_t
hash_fnv_lower(Str* str)
{
	auto pos = (const unsigned char*)str->pos;
	auto end = (const unsigned char*)str->end;
	uint32_t hash = 2166136261;
	while (pos < end)
	{
		hash = (hash * 16777619) ^ tolower(*pos);
		pos++;
	}
	return hash;
}

hot static inline uint32_t
hash_murmur_scramble(uint32_t k)
{
	k *= 0xcc9e2d51;
	k = (k << 15) | (k >> 17);
	k *= 0x1b873593;
	return k;
}

hot static inline uint32_t
hash_murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
	uint32_t h = seed;
	uint32_t k;

	for (size_t i = len >> 2; i; i--)
	{
		memcpy(&k, key, sizeof(uint32_t));
		key += sizeof(uint32_t);
		h ^= hash_murmur_scramble(k);
		h = (h << 13) | (h >> 19);
		h = h * 5 + 0xe6546b64;
	}

	k = 0;
	for (size_t i = len & 3; i; i--)
	{
		k <<= 8;
		k |= key[i - 1];
	}

	h ^= hash_murmur_scramble(k);
	h ^= len;
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}
