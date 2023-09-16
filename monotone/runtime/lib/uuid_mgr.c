
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>

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
uuid_mgr_init(UuidMgr* self)
{
	self->seed[0] = 0;
	self->seed[1] = 0;
	spinlock_init(&self->lock);
}

void
uuid_mgr_free(UuidMgr* self)
{
	spinlock_free(&self->lock);
}

void
uuid_mgr_open(UuidMgr* self)
{
	uint64_t seed_random_dev[2] = { 0, 0 };

	int fd;
	fd = vfs_open("/dev/urandom", O_RDONLY, 0644);
	if (unlikely(fd != -1))
	{
		vfs_read(fd, seed_random_dev, sizeof(uint64_t) * 2);
		vfs_close(fd);
	}

	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);

	uint64_t seed;
	seed = getpid() ^ getuid() ^ ts.tv_sec ^ ts.tv_nsec;
	seed_random_dev[0] ^= seed ^ rand();
	seed_random_dev[1] ^= seed ^ rand();

	self->seed[0] = seed_random_dev[0];
	self->seed[1] = seed_random_dev[1];
}

static inline uint64_t
uuid_xorshift128plus(UuidMgr* self)
{
	uint64_t a = self->seed[0];
	uint64_t b = self->seed[1];
	self->seed[0] = b;
	a ^= a << 23;
	self->seed[1] = a ^ b ^ (a >> 17) ^ (b >> 26);
	return self->seed[1] + b;
}

void
uuid_mgr_generate(UuidMgr* self, Uuid* uuid)
{
	spinlock_lock(&self->lock);
	uuid->a = uuid_xorshift128plus(self);
	uuid->b = uuid_xorshift128plus(self);
	spinlock_unlock(&self->lock);
}

uint64_t
uuid_mgr_random(UuidMgr* self)
{
	Uuid uuid;
	uuid_mgr_generate(self, &uuid);
	return uuid.a ^ uuid.b;
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

static inline int
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
