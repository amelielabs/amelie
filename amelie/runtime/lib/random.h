#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Random Random;

struct Random
{
	atomic_u64 seed[2];
};

void     random_init(Random*);
void     random_open(Random*);
uint64_t random_generate(Random*);
void     random_generate_alnum(Random*, uint8_t*, int);
