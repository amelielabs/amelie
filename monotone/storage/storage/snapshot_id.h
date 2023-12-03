#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SnapshotId SnapshotId;

struct SnapshotId
{
	Uuid     uuid;
	int64_t  min;
	int64_t  max;
	uint64_t lsn;
	bool     incomplete;
};

static inline void
snapshot_id_init(SnapshotId* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
snapshot_id_set(SnapshotId* self, Uuid* uuid,
                int64_t min,
                int64_t max, uint64_t lsn)
{
	self->uuid = *uuid;
	self->min  =  min;
	self->max  =  max;
	self->lsn  =  lsn;
}

static inline void
snapshot_id_path(SnapshotId* self, char* path, bool incomplete)
{
	// <base>/storage_uuid/min.max.lsn[.incomplete]
	char uuid[UUID_SZ];
	uuid_to_string(&self->uuid, uuid, sizeof(uuid));

	snprintf(path, PATH_MAX,
	         "%s/%s/%020" PRIi64 ".%020" PRIi64 ".%020" PRIu64 "%s",
	         config_directory(),
	         uuid,
	         self->min,
	         self->max,
	         self->lsn,
	         incomplete ? ".incomplete" : "");
}

static inline void
snapshot_id_name(SnapshotId* self, char* path, bool incomplete)
{
	// min.max.lsn[.incomplete]
	snprintf(path, PATH_MAX,
	         "%020" PRIi64 ".%020" PRIi64 ".%020" PRIu64 "%s",
	         self->min,
	         self->max,
	         self->lsn,
	         incomplete ? ".incomplete" : "");
}
