#pragma once

//
// indigo
//
// SQL OLTP database
//

static inline void*
in_malloc_nothrow(size_t size)
{
	return malloc(size);
}

static inline void*
in_realloc_nothrow(void* pointer, size_t size)
{
	return realloc(pointer, size);
}

static inline void
in_free(void* pointer)
{
	free(pointer);
}
