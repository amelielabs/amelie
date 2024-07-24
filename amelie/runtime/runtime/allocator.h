#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline void*
am_malloc_nothrow(size_t size)
{
	return malloc(size);
}

static inline void*
am_realloc_nothrow(void* pointer, size_t size)
{
	return realloc(pointer, size);
}

static inline void
am_free(void* pointer)
{
	free(pointer);
}
