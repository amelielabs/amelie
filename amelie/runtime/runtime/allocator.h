#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline void*
am_malloc(size_t size)
{
	auto ptr = malloc(size);
	if (unlikely(ptr == NULL))
		oom();
	return ptr;
}

static inline void*
am_realloc(void* pointer, size_t size)
{
	auto ptr = realloc(pointer, size);
	if (unlikely(ptr == NULL))
		oom();
	return ptr;
}

static inline void
am_free(void* pointer)
{
	free(pointer);
}
