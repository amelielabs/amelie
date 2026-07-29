
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

#include <amelie_base.h>
#include <amelie_io.h>
#include <amelie_lib.h>

int
uuid_set_nothrow(Uuid* self, Str* src)
{
	if (unlikely(str_size(src) < (UUID_SZ - 1)))
		return -1;

	uint64_t a     = 0;
	uint64_t b     = 0;
	uint64_t value = 0;

	auto string = str_of(src);
	for (int i = 0; i < 36; i++)
	{
		switch (i) {
		case 8:
			if (unlikely(string[i] != '-'))
				return -1;
			a |= (value << 32);
			value = 0;
			break;
		case 13:
			if (unlikely(string[i] != '-'))
				return -1;
			a |= (value << 16);
			value = 0;
			break;
		case 18:
			if (unlikely(string[i] != '-'))
				return -1;
			a |= value;
			value = 0;
			break;
		case 23:
			if (unlikely(string[i] != '-'))
				return -1;
			b |= (value << 48);
			value = 0;
			break;
		default:
		{
			uint8_t byte = string[i];
			if (byte >= '0' && byte <= '9')
				byte = byte - '0';
			else
			if (byte >= 'a' && byte <= 'f')
				byte = byte - 'a' + 10;
			else
			if (byte >= 'A' && byte <= 'F')
				byte = byte - 'A' + 10;
			else
				return -1;
			value = (value << 4) | byte;
			break;
		}
		}
	}

	// last 12 hex chars (48 bits)
	b |= (value & 0xFFFFFFFFFFFFULL);

	self->a = a;
	self->b = b;
	return 0;
}

void
uuid_set(Uuid* self, Str* src)
{
	int rc = uuid_set_nothrow(self, src);
	if (unlikely(rc == -1))
		error("failed to parse uuid");
}

void
uuid_get(Uuid* self, char* string, int size)
{
	assert(size >= UUID_SZ);
	format(string, size, "{08x}-{04x}-{04x}-{04x}-{012llx}",
	       (uint32_t)(self->a >> 32),
	       (uint16_t)(self->a >> 16),
	       (uint16_t)(self->a),
	       (uint16_t)(self->b >> 48),
	       (unsigned long long)(self->b & 0xFFFFFFFFFFFFULL));
}

hot void
uuid_generate_as(Uuid* self, uint64_t seed, uint64_t time_ms)
{
	// RFC 9562 (UUIDv7)

	// get 12 bits from top of the seed for rand_a
	uint64_t rand_a = (seed >> 52) & 0xFFFULL;

	// high 64 bits: [48-bit timestamp] [4-bit version 0b0111] [12-bit rand_a]
	self->a = ((time_ms & 0xFFFFFFFFFFFFULL) << 16)
	          | (0x7ULL << 12)
	          | rand_a;

	// derive the missing 10 bits by scrambling seed with time_ms
	uint64_t rand_b_10 = ((seed * 0x9e3779b97f4a7c15ULL) ^ time_ms) & 0x3FFULL;

	// 52 remaining random bits from the seed
	uint64_t rand_b_52 = seed & 0x000FFFFFFFFFFFFFULL;
	uint64_t rand_b = (rand_b_10 << 52) | rand_b_52;

	// low 64 bits: [2-bit variant 0b10] [62-bit rand_b]
	self->b = (rand_b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
}

void
uuid_generate(Uuid* self, Random* random, uint64_t time_ms)
{
	auto seed = random_generate(random);
	uuid_generate_as(self, seed, time_ms);
}
