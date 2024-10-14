#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Vector Vector;

struct Vector
{
	int    size;
	float* value;
};

static inline void
vector_init(Vector* self, int size, float* value)
{
	self->size  = size;
	self->value = value;
}

int    vector_compare(Vector*, Vector*);
void   vector_add(Vector*, Vector*, Vector*);
void   vector_sub(Vector*, Vector*, Vector*);
void   vector_mul(Vector*, Vector*, Vector*);
double vector_distance(Vector*, Vector*);
