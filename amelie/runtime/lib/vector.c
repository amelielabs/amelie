
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

hot int
vector_compare(int adim, int bdim, float* a, float* b)
{
	auto rc = compare_int32(adim, bdim);
	if (rc != 0)
		return rc;
	const float* restrict va = a;
	const float* restrict vb = b;
	for (auto i = 0; i < adim; i++)
	{
		rc = compare_float(va[i], vb[i]);
		if (rc != 0)
			return rc;
	}
	return 0;
}

hot void
vector_add(int dim, float* result, float* a, float* b)
{
	float* restrict vr = result;
	const float* restrict va = a;
	const float* restrict vb = b;
	for (auto i = 0; i < dim; i++)
		vr[i] = va[i] + vb[i];
}

hot void
vector_sub(int dim, float* result, float* a, float* b)
{
	float* restrict vr = result;
	const float* restrict va = a;
	const float* restrict vb = b;
	for (auto i = 0; i < dim; i++)
		vr[i] = va[i] - vb[i];
}

hot void
vector_mul(int dim, float* result, float* a, float* b)
{
	float* restrict vr = result;
	const float* restrict va = a;
	const float* restrict vb = b;
	for (auto i = 0; i < dim; i++)
		vr[i] = va[i] * vb[i];
}

hot double
vector_distance(int dim, float* a, float* b)
{
	const float* restrict va = a;
	const float* restrict vb = b;

	float ab = 0.0f;
	float a2 = 0.0f;
	float b2 = 0.0f;

	for (auto i = 0; i < dim; i++)
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
