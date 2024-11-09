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

typedef struct Vector Vector;

struct Vector
{
	uint32_t size;
	float    value[];
} packed;

void   vector_init(Vector*, int);
int    vector_compare(Vector*, Vector*);
void   vector_add(Vector*, Vector*, Vector*);
void   vector_sub(Vector*, Vector*, Vector*);
void   vector_mul(Vector*, Vector*, Vector*);
double vector_distance(Vector*, Vector*);

static inline int
vector_size(Vector* self)
{
	return sizeof(Vector) + self->size * sizeof(float);
}
