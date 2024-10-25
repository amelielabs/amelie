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

typedef struct Random Random;

struct Random
{
	atomic_u64 seed[2];
};

void     random_init(Random*);
void     random_open(Random*);
uint64_t random_generate(Random*);
void     random_generate_alnum(Random*, uint8_t*, int);
