
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
vector_init(Vector* self, int size)
{
	self->size = size;
}

hot int
vector_compare(Vector* a, Vector* b)
{
	auto rc = compare_int32(a->size, b->size);
	if (rc != 0)
		return rc;
	for (uint32_t i = 0; i < a->size; i++)
	{
		rc = compare_float(a->value[i], b->value[i]);
		if (rc != 0)
			return rc;
	}
	return 0;
}

hot void
vector_add(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (uint32_t i = 0; i < a->size; i++)
		result->value[i] = a->value[i] + b->value[i];
}

hot void
vector_sub(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (uint32_t i = 0; i < a->size; i++)
		result->value[i] = a->value[i] - b->value[i];
}

hot void
vector_mul(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (uint32_t i = 0; i < a->size; i++)
		result->value[i] = a->value[i] * b->value[i];
}

hot double
vector_distance(Vector* a, Vector* b)
{
	assert(a->size == b->size);
	// calculate consine similarity
	double ab = 0.0;
	double a2 = 0.0;
	double b2 = 0.0;
	for (uint32_t i = 0; i < a->size; i++)
	{
		ab += a->value[i] * b->value[i];
		a2 += a->value[i] * a->value[i];
		b2 += b->value[i] * b->value[i];
	}
	double Sc = ab / sqrt(a2 * b2);
	if (Sc < -1.0)
		Sc = -1.0;
	else
	if (Sc > 1.0)
		Sc = 1.0;
	// calculate consine distance
	double Dc = 1.0 - Sc;
	return Dc;
}
