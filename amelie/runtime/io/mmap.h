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

typedef struct Mmap Mmap;

struct Mmap
{
	Str mmap;
};

static inline void
mmap_init(Mmap* self)
{
	str_init(&self->mmap);
}

static inline void
mmap_unmap(Mmap* self)
{
	if (self->mmap.pos)
		munmap(self->mmap.pos, str_size(&self->mmap));
	str_init(&self->mmap);
}

static inline void
mmap_file(Mmap* self, File* file)
{
	assert(str_empty(&self->mmap));
	auto pointer = mmap(NULL, file->size, PROT_READ, MAP_PRIVATE, file->fd, 0);
	if (pointer == MAP_FAILED)
		error_system();
	str_set(&self->mmap, pointer, file->size);

	auto map = &self->mmap;
	map->pos       = pointer;
	map->end       = pointer + file->size;
	map->allocated = false;
}
