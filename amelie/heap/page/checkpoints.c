
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

#include <amelie_runtime>
#include <amelie_type.h>
#include <amelie_page.h>

static inline CheckpointsRef*
checkpoints_ref_allocate(uint64_t id)
{
	auto self = (CheckpointsRef*)am_malloc(sizeof(CheckpointsRef));
	self->id   = id;
	self->refs = 0;
	list_init(&self->link);
	return self;
}

static inline void
checkpoints_ref_free(CheckpointsRef* self)
{
	am_free(self);
}

void
checkpoints_init(Checkpoints* self, Uuid* id)
{
	self->current    = NULL;
	self->id         = *id;
	self->list_count = 0;
	list_init(&self->list);
}

void
checkpoints_free(Checkpoints* self)
{
	list_foreach_safe(&self->list)
	{
		auto cp = list_at(CheckpointsRef, link);
		checkpoints_ref_free(cp);
	}
}

static inline int
checkpoint_id_of(const char* name, uint64_t* lsn,  bool* incomplete)
{
	// <lsn>[.incomplete]
	*lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(uint64_mul_add_overflow(lsn, *lsn, 10, *name - '0')))
			return -1;
		name++;
	}
	if (! *name)
	{
		*incomplete = false;
		return 0;
	}
	*incomplete = !strcmp(name, ".incomplete");
	if (*incomplete)
		return 0;
	return -1;
}

void
checkpoints_open(Checkpoints* self)
{
	// create directory
	char uuid[UUID_SZ];
	char path[PATH_MAX];
	uuid_get(&self->id, uuid, sizeof(uuid));
	format(path, sizeof(path), "{s}/storage/{s}",
	       state_directory(), uuid);

	// ensure directory exists
	if (! fs_exists("{s}", path))
		return;

	// read directory, get a list of checkpoints
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("db: directory '{s}' open error", path);
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		bool incomplete = false;
		uint64_t lsn;
		if (checkpoint_id_of(entry->d_name, &lsn, &incomplete) == -1)
			continue;

		// remove incomplete checkpoint
		if (incomplete)
		{
			info("db: removing incomplete checkpoint: '{s}/{s}'",
			     path, entry->d_name);
			fs_unlink_if_exists("{s}/{u64}.incomplete", path, lsn);
			fs_unlink_if_exists("{s}/{u64}.sql", path, lsn);
			continue;
		}

		checkpoints_add(self, lsn);
	}
}

void
checkpoints_gc(Checkpoints* self)
{
	(void)self;
	// todo: remove all files which has no alive page references
}

void
checkpoints_add(Checkpoints* self, uint64_t lsn)
{
	auto ref = checkpoints_ref_allocate(lsn);

	list_append(&self->list, &ref->link);
	self->list_count++;

	// track last checkpoint
	if (!self->current || ref->id > self->current->id)
		self->current = ref;
}

void
checkpoints_cleanup(Uuid* id)
{
	char uuid[UUID_SZ];
	uuid_get(id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	char path_drop[PATH_MAX];
	format(path, sizeof(path), "{s}/storage/{s}",
	       state_directory(), uuid);
	format(path_drop, sizeof(path_drop), "{s}.drop", path);

	// ensure directory exists
	if (! fs_exists("{s}", path))
		return;

	// rename and drop
	fs_rename(path, "{s}", path_drop);
	fs_rmdir("{s}", path_drop);

	// sync
	if (opt_int_of(&config()->storage_sync))
		fs_syncdir("{s}/storage", state_directory());
}
