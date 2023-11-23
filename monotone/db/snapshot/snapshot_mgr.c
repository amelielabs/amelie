
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>

static inline Snapshot*
snapshot_allocate(Uuid* uuid, uint64_t id)
{
	auto self = (Snapshot*)mn_malloc(sizeof(Snapshot));
	self->id        = id;
	self->uuid      = *uuid;
	self->uuid_next = NULL;
	list_init(&self->link);
	return self;
}

static inline void
snapshot_free(Snapshot* self)
{
	auto snapshot = self;
	while (snapshot)
	{
		auto next = snapshot->uuid_next;
		mn_free(snapshot);
		snapshot = next;
	}
}

void
snapshot_mgr_init(SnapshotMgr* self)
{
	list_init(&self->list);
	id_mgr_init(&self->refs);
	mutex_init(&self->lock);
}

void
snapshot_mgr_free(SnapshotMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto snapshot = list_at(Snapshot, link);
		snapshot_free(snapshot);
	}
	id_mgr_free(&self->refs);
	mutex_free(&self->lock);
}

enum
{
	SNAPSHOT_UNDEF,
	SNAPSHOT,
	SNAPSHOT_INCOMPLETE,
};

static inline int
snapshot_mgr_read_name(char* name, Uuid* uuid, uint64_t* id)
{
	// uuid.id
	// uuid.id.incomplete

	// uuid + '.'
	int len = strlen(name);
	if (len <= UUID_SZ)
		return SNAPSHOT_UNDEF;

	// read uuid
	Str str;
	str_init(&str);
	str_set(&str, name, UUID_SZ - 1);
	if (uuid_from_string_nothrow(uuid, &str) == -1)
		return SNAPSHOT_UNDEF;

	// '.'
	const char* pos = name + UUID_SZ;
	if (*pos != '.')
		return SNAPSHOT_UNDEF;
	pos++;

	// read id
	*id = 0;
	while (*pos && *pos != '.')
	{
		if (unlikely(! isdigit(*pos)))
			return -1;
		*id = (*id * 10) + *pos - '0';
		pos++;
	}

	if (likely(! *pos))
		return SNAPSHOT;

	// .incomplete
	if (! strcmp(pos, ".incomplete"))
		return SNAPSHOT_INCOMPLETE;
	
	return SNAPSHOT_UNDEF;
}

void
snapshot_mgr_open(SnapshotMgr* self)
{
	// create snapshot directory
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot", config_directory());
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// open and read log directory
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("wal: directory '%s' open error", path);
	guard(dir_guard, closedir, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		Uuid     uuid;
		uint64_t id;
		switch (snapshot_mgr_read_name(entry->d_name, &uuid, &id)) {
		case SNAPSHOT_UNDEF:
			break;
		case SNAPSHOT_INCOMPLETE:
			// remove incomplete snapshot file
			fs_unlink("%s/snapshot/%s", config_directory(), entry->d_name);
			break;
		case SNAPSHOT:
			snapshot_mgr_add(self, &uuid, id);
			break;
		}
	}
}

void
snapshot_mgr_add(SnapshotMgr* self, Uuid* uuid, uint64_t id)
{
	auto snapshot = snapshot_allocate(uuid, id);

	mutex_lock(&self->lock);
	guard(mutex_guard, mutex_unlock, &self->lock);

	// find previous snapshot
	list_foreach(&self->list)
	{
		auto ref = list_at(Snapshot, link);
		if (! uuid_compare(uuid, &ref->uuid))
			continue;

		// todo: add in order

		// relink previous snapshot
		list_unlink(&ref->link);
		list_init(&ref->link);
		snapshot->uuid_next = ref;
		break;
	}

	list_append(&self->list, &snapshot->link);
}

static inline void
snapshot_mgr_unlink(Snapshot* self)
{
	auto snapshot = self;
	while (snapshot)
	{
		auto next = snapshot->uuid_next;

		char uuid[UUID_SZ];
		uuid_to_string(&snapshot->uuid, uuid, sizeof(uuid));

		log("snapshot: remove previous snapshot %s.%020" PRIu64,
		    uuid,
			self->id);

		fs_unlink("%s/snapshot/%s.%020" PRIu64,
		          config_directory(),
		          uuid,
		          self->id);

		mn_free(snapshot);

		snapshot = next;
	}
}

void
snapshot_mgr_gc(SnapshotMgr* self, uint64_t id_snapshot)
{
	mutex_lock(&self->lock);
	guard(mutex_guard, mutex_unlock, &self->lock);

	uint64_t min_ref = id_mgr_min(&self->refs);
	uint64_t min = id_snapshot;
	if (min_ref < min)
		min = min_ref;

	// remove previous snapshots
	list_foreach(&self->list)
	{
		auto snapshot = list_at(Snapshot, link);

		// go though previous versions
		snapshot = snapshot->uuid_next;
		while (snapshot)
		{
			if (snapshot->id >= min)
			{
				snapshot = snapshot->uuid_next;
				continue;
			}

			snapshot_mgr_unlink(snapshot);
			break;
		}
	}
}
