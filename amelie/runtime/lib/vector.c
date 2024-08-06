
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

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
