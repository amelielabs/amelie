#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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

void vector_add(Vector*, Vector*, Vector*);
void vector_sub(Vector*, Vector*, Vector*);
void vector_mul(Vector*, Vector*, Vector*);
int  vector_compare(Vector*, Vector*);
