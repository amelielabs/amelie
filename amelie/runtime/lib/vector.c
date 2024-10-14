
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

hot int
vector_compare(Vector* a, Vector* b)
{
	if (a->size < b->size)
		return -1;
	if (a->size > b->size)
		return 1;
	for (int i = 0; i < a->size; i++)
	{
		if (a->value[i] < b->value[i])
			return -1;
		if (a->value[i] > b->value[i])
			return 1;
	}
	return 0;
}

hot void
vector_add(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (int i = 0; i < a->size; i++)
		result->value[i] = a->value[i] + b->value[i];
}

hot void
vector_sub(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (int i = 0; i < a->size; i++)
		result->value[i] = a->value[i] - b->value[i];
}

hot void
vector_mul(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	for (int i = 0; i < a->size; i++)
		result->value[i] = a->value[i] * b->value[i];
}

hot double
vector_distance(Vector* a, Vector* b)
{
	// calculate consine similarity
	double ab = 0.0;
	double a2 = 0.0;
	double b2 = 0.0;
	for (int i = 0; i < a->size; i++)
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
