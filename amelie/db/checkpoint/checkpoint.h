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

typedef struct Checkpoint Checkpoint;

struct Checkpoint
{
	uint64_t id;
	int      refs;
	List     link;
};

static inline Checkpoint*
checkpoint_allocate(uint64_t id)
{
	auto self = (Checkpoint*)am_malloc(sizeof(Checkpoint));
	self->id   = id;
	self->refs = 0;
	list_init(&self->link);
	return self;
}

static inline void
checkpoint_free(Checkpoint* self)
{
	am_free(self);
}

static inline void
checkpoint_ref(Checkpoint* self)
{
	self->refs++;
}

static inline void
checkpoint_unref(Checkpoint* self)
{
	self->refs--;
	assert(self->refs >= 0);
}

#if 0
static inline int64_t
part_id_of(const char* name)
{
	int64_t id = 0;
	while (*name)
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(&id, id, 10, *name - '0')))
			return -1;
		name++;
	}
	return id;
}

void
checkpoint_mgr_list(CheckpointMgr* self, uint64_t checkpoint, Buf* buf)
{
	unused(self);
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/checkpoints/%" PRIu64,
	     state_directory(), checkpoint);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_opendir_defer, dir);

	// read partitions to create a sorted list
	IdMgr list;
	id_mgr_init(&list);
	defer(id_mgr_free, &list);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		auto id = part_id_of(entry->d_name);
		if (id == -1)
			continue;
		id_mgr_add(&list, id);
	}

	// write the list of partitions
	for (int i = 0; i < list.list_count; i++)
	{
		auto id = buf_u64(&list.list)[i];
		encode_array(buf);
		// path
		sfmt(path, sizeof(path), "checkpoints/%" PRIu64 "/%" PRIu64,
		     checkpoint, id);
		encode_cstr(buf, path);
		// size
		auto size = fs_size("%s/checkpoints/%" PRIu64 "/%" PRIu64, state_directory(),
		                    checkpoint, id);
		if (size == -1)
			error_system();
		encode_integer(buf, size);
		encode_array_end(buf);
	}

	// add catalog.json file last
	encode_array(buf);
	// path
	sfmt(path, sizeof(path), "checkpoints/%" PRIu64 "/catalog.json", checkpoint);
	encode_cstr(buf, path);
	// size
	auto size = fs_size("%s/checkpoints/%" PRIu64 "/catalog.json",
	                    state_directory(),
	                    checkpoint);
	if (size == -1)
		error_system();
	encode_integer(buf, size);
	encode_array_end(buf);
}
#endif
