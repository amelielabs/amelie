
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
#include <monotone_snapshot.h>

void
snapshot_init(Snapshot* self)
{
	self->lsn = UINT64_MAX;
	memset(self->uuid, 0, sizeof(self->uuid));
	file_init(&self->file);
}

void
snapshot_create(Snapshot* self, Uuid* uuid, uint64_t lsn)
{
	self->lsn = lsn;

	// <base>/snapshot/storage_uuid.lsn.incomplete
	uuid_to_string(uuid, self->uuid, sizeof(self->uuid));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot/%s.%020" PRIu64 ".incomplete",
	         config_directory(),
	         self->uuid,
	         lsn);

	// create
	file_create(&self->file, path);

	log("snapshot: begin %s.%020" PRIu64 ".incomplete",
	    self->uuid,
	    lsn);
}

void
snapshot_complete(Snapshot* self)
{
	// rename incomplete snapshot file to <base>/snapshot/storage_uuid.lsn
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/snapshot/%s.%020" PRIu64,
	         config_directory(),
	         self->uuid,
	         self->lsn);

	// sync
	file_sync(&self->file);

	// rename
	file_rename(&self->file, path);
	file_close(&self->file);

	log("snapshot: complete %s.%020" PRIu64,
	    self->uuid,
	    self->lsn);
}

void
snapshot_delete(Snapshot* self)
{
	file_close(&self->file);

	fs_unlink("%s/snapshot/%s.%020" PRIu64 ".incomplete",
	          config_directory(),
	          self->uuid,
	          self->lsn);
}

void
snapshot_write(Snapshot* self, Iov* iov)
{
	file_writev(&self->file, iov_pointer(iov), iov->iov_count);
}
