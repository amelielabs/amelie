
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

#include <amelie_base.h>
#include <amelie_os.h>
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
	const uint32_t size = a->size;
	const float* restrict va = a->value;
	const float* restrict vb = b->value;
	for (uint32_t i = 0; i < size; i++)
	{
		rc = compare_float(va[i], vb[i]);
		if (rc != 0)
			return rc;
	}
	return 0;
}

hot void
vector_add(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	const uint32_t size = a->size;
	float* restrict vr = result->value;
	const float* restrict va = a->value;
	const float* restrict vb = b->value;
	for (uint32_t i = 0; i < size; i++)
		vr[i] = va[i] + vb[i];
}

hot void
vector_sub(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	const uint32_t size = a->size;
	float* restrict vr = result->value;
	const float* restrict va = a->value;
	const float* restrict vb = b->value;
	for (uint32_t i = 0; i < size; i++)
		vr[i] = va[i] - vb[i];
}

hot void
vector_mul(Vector* result, Vector* a, Vector* b)
{
	assert(result->size == a->size);
	const uint32_t size = a->size;
	float* restrict vr = result->value;
	const float* restrict va = a->value;
	const float* restrict vb = b->value;
	for (uint32_t i = 0; i < size; i++)
		vr[i] = va[i] * vb[i];
}

hot double
vector_distance(Vector* a, Vector* b)
{
	assert(a->size == b->size);
	const uint32_t size = a->size;
	const float* restrict va = a->value;
	const float* restrict vb = b->value;

	float ab = 0.0f;
	float a2 = 0.0f;
	float b2 = 0.0f;

	for (uint32_t i = 0; i < size; i++)
	{
		float avi = va[i];
		float bvi = vb[i];
		ab += avi * bvi;
		a2 += avi * avi;
		b2 += bvi * bvi;
	}

	if (unlikely(a2 <= 0.0f || b2 <= 0.0f))
		return 1.0;

	double Sc = (double)ab / sqrt((double)a2 * (double)b2);
	if (Sc < -1.0)
		Sc = -1.0;
	else
	if (Sc > 1.0)
		Sc = 1.0;

	return 1.0 - Sc;
}
