
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

void
snapshot_file_init(SnapshotFile* self)
{
	self->id = UINT64_MAX;
	memset(self->uuid, 0, sizeof(self->uuid));
	file_init(&self->file);
}

void
snapshot_file_create(SnapshotFile* self, Uuid* uuid, uint64_t id)
{
	self->id = id;

	// <base>/snapshot/uuid.id.incomplete
	uuid_to_string(uuid, self->uuid, sizeof(self->uuid));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot/%s.%020" PRIu64 ".incomplete",
	         config_directory(),
	         self->uuid,
	         id);

	// create
	file_create(&self->file, path);

	log("snapshot: begin %s.%020" PRIu64 ".incomplete",
	    self->uuid,
	    id);
}

void
snapshot_file_complete(SnapshotFile* self)
{
	// rename incomplete file to <base>/snapshot/uuid.id
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot/%s.%020" PRIu64,
	         config_directory(),
	         self->uuid,
	         self->id);

	// sync
	file_sync(&self->file);

	// rename
	file_rename(&self->file, path);
	file_close(&self->file);

	log("snapshot: complete %s.%020" PRIu64,
	    self->uuid,
	    self->id);
}

void
snapshot_file_abort(SnapshotFile* self)
{
	file_close(&self->file);

	fs_unlink("%s/snapshot/%s.%020" PRIu64 ".incomplete",
	          config_directory(),
	          self->uuid,
	          self->id);
}

void
snapshot_file_write(SnapshotFile* self, Iov* iov)
{
	file_writev(&self->file, iov_pointer(iov), iov->iov_count);
}
