
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>

typedef struct uuid_bits uuid_bits_t;

struct uuid_bits
{
	uint32_t a;
	uint16_t b;
	uint16_t c;
	uint16_t d;
	uint16_t e;
	uint32_t f;
} packed;

void
uuid_init(Uuid* self)
{
	self->a = 0;
	self->b = 0;
}

void
uuid_generate(Uuid* self, Random* random)
{
	self->a = random_generate(random);
	self->b = random_generate(random);
}

void
uuid_to_string(Uuid* self, char* string, int size)
{
	assert(size >= UUID_SZ);

	auto bits = (uuid_bits_t*)self;
	snprintf(string, size, "%08x-%04x-%04x-%04x-%04x%08x",
	         bits->a, bits->b, bits->c,
	         bits->d, bits->e, bits->f);
}

int
uuid_from_string_nothrow(Uuid* self, Str* src)
{
	if (unlikely(str_size(src) < (UUID_SZ - 1)))
		return -1;

	auto string = str_of(src);
	auto bits = (uuid_bits_t*)self;

	// convert
	uint64_t value = 0;
	int i = 0;
	for (; i < 36; i++)
	{
		switch (i) {
		case 8:
			if (unlikely(string[i] != '-'))
				return -1;
			bits->a = value;
			value = 0;
			break;
		case 13:
			if (unlikely(string[i] != '-'))
				return -1;
			bits->b = value;
			value = 0;
			break;
		case 18:
			if (unlikely(string[i] != '-'))
				return -1;
			bits->c = value;
			value = 0;
			break;
		case 23:
			if (unlikely(string[i] != '-'))
				return -1;
			bits->d = value;
			value = 0;
			break;
		case 28:
			bits->e = value;
			value = 0;
			// fallthrough
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
			value = (value << 4) | (byte & 0xF);
			break;
		}
		}
	}
	bits->f = value;
	return 0;
}

void
uuid_from_string(Uuid* self, Str* src)
{
	int rc = uuid_from_string_nothrow(self, src);
	if (unlikely(rc == -1))
		error("%s", "failed to parse uuid");
}
