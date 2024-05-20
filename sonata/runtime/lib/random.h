#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Random Random;

struct Random
{
	Spinlock lock;
	uint64_t seed[2];
};

void     random_init(Random*);
void     random_free(Random*);
void     random_open(Random*);
uint64_t random_generate(Random*);
void     random_generate_alnum(Random*, uint8_t*, int);
