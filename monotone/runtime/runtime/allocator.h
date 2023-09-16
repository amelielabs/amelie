#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline void*
mn_malloc_nothrow(int size)
{
	return malloc(size);
}

static inline void*
mn_realloc_nothrow(void* pointer, int size)
{
	return realloc(pointer, size);
}

static inline void
mn_free(void* pointer)
{
	free(pointer);
}
