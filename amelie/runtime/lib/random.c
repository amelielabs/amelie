
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

void
random_init(Random* self)
{
	self->seed[0] = 0;
	self->seed[1] = 0;
}

void
random_open(Random* self)
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

hot uint64_t
random_generate(Random* self)
{
	// xorshift128plus
	auto seed = self->seed;
	uint64_t a = atomic_u64_of(&seed[0]);
	uint64_t b = atomic_u64_of(&seed[1]);
	atomic_u64_set(&seed[0], b);
	a ^= a << 23;
	uint64_t c = a ^ b ^ (a >> 17) ^ (b >> 26);
	atomic_u64_set(&seed[1], c);
	return c + b;
}

void
random_generate_alnum(Random* self, uint8_t* data, int data_size)
{
	int data_pos = 0;
	for (;;)
	{
		uint64_t rnd = random_generate(self);
		uint8_t* pos = (uint8_t*)&rnd;
		for (int i = 0; i < 8; i++)
		{
			if (! isalnum(pos[i]))
				continue;
			data[data_pos] = pos[i];
			data_pos++;
			if (data_pos == data_size)
				return;
		}
	}
}
