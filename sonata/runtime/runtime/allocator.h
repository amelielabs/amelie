#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

static inline void*
so_malloc_nothrow(size_t size)
{
	return malloc(size);
}

static inline void*
so_realloc_nothrow(void* pointer, size_t size)
{
	return realloc(pointer, size);
}

static inline void
so_free(void* pointer)
{
	free(pointer);
}
