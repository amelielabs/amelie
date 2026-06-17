#pragma once

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

static inline void*
am_malloc(size_t size)
{
	auto ptr = malloc(size);
	if (unlikely(ptr == NULL))
		oom();
	return ptr;
}

static inline void*
am_malloc_aligned_nothrow(size_t size, int alignment)
{
	void* ptr = NULL;
	if (unlikely(posix_memalign(&ptr, alignment, size) != 0))
		return NULL;
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
