
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>

void
checkpoint_init(Checkpoint* self, CheckpointMgr* mgr)
{
	self->catalog       = NULL;
	self->lsn           = 0;
	self->workers       = NULL;
	self->workers_count = 0;
	self->rr            = 0;
	self->mgr           = mgr;
}

void
checkpoint_free(Checkpoint* self)
{
	if (self->workers)
	{
		for (int i = 0; i < self->workers_count; i++)
		{
			auto worker = &self->workers[i];
			if (worker->on_complete)
				condition_free(worker->on_complete);
		}
		am_free(self->workers);
	}
	self->workers = NULL;
	self->workers_count = 0;
	if (self->catalog)
		buf_free(self->catalog);
}

void
checkpoint_begin(Checkpoint* self, uint64_t lsn, int workers)
{
	// prepare catalog data
	auto mgr = self->mgr;
	self->catalog = mgr->iface->catalog_dump(mgr->iface_arg);

	// prepare workers
	self->lsn = lsn;
	self->workers_count = workers;
	self->workers = am_malloc(sizeof(CheckpointWorker) * workers);
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		worker->pid         = -1;
		worker->on_complete = condition_create();
		worker->list_count  = 0;
		list_init(&worker->list);
	}

	// add partitions
	mgr->iface->add(self, mgr->iface_arg);
}

void
checkpoint_add(Checkpoint* self, Part* part)
{
	// distribute among workers
	if (self->rr == self->workers_count)
		self->rr = 0;
	int order = self->rr++;
	auto worker = &self->workers[order];

	list_init(&part->link_cp);
	list_append(&worker->list, &part->link_cp);
	worker->list_count++;
}

hot static bool
checkpoint_worker_main(Checkpoint* self, CheckpointWorker* worker)
{
	Snapshot snapshot;
	snapshot_init(&snapshot);

	Exception e;
	if (enter(&e))
	{
		// create primary index snapshot per partition
		list_foreach(&worker->list)
		{
			auto part = list_at(Part, link_cp);
			snapshot_reset(&snapshot);
			snapshot_create(&snapshot, part, self->lsn);
		}
	}
	snapshot_free(&snapshot);

	if (leave(&e))
	{ }

	return e.triggered;
}

static void
checkpoint_worker_run(Checkpoint* self, CheckpointWorker* worker)
{
	// create new process
	worker->pid = fork();
	switch (worker->pid) {
	case -1:
		error_system();
	case  0:
		break;
	default:
		return;
	}

	// create snapshots
	auto error = checkpoint_worker_main(self, worker);

	// signal waiter process
	condition_signal(worker->on_complete);

	// done

	// valgrind hack.
	//
	// When using _exit(2) valgrind would complain about
	// memory not being freed in the child.
	//
	// We use a simple hack that instead executes another
	// app which returns result code.
	//
	if (! error)
		execl("/bin/true", "/bin/true", NULL);
	execl("/bin/false", "/bin/false", NULL);
}

static bool
checkpoint_worker_wait(CheckpointWorker* self)
{
	condition_wait(self->on_complete, -1);
	condition_free(self->on_complete);
	self->on_complete = NULL;

	int status = 0;
	int rc = waitpid(self->pid, &status, 0);
	if (rc == -1)
		error_system();

	bool failed = false;
	if (WIFEXITED(status))
	{
		if (WEXITSTATUS(status) == EXIT_FAILURE)
			failed = true;
	} else {
		failed = true;
	}

	return failed;
}

static void
checkpoint_create_catalog(Checkpoint* self)
{
	// create <base>/<lsn>.incomplete/catalog
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%" PRIu64 ".incomplete/catalog",
	         config_directory(), self->lsn);

	// convert catalog to json
	Buf text;
	buf_init(&text);
	guard(buf_free, &text);
	uint8_t* pos = self->catalog->start;
	json_export_pretty(&text, global()->timezone, &pos);

	// create config file
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, &text);
}

void
checkpoint_run(Checkpoint* self)
{
	// create <base>/<lsn>.incomplete
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%" PRIu64 ".incomplete",
	         config_directory(), self->lsn);

	info("checkpoint %" PRIu64 ": using %d workers", self->lsn,
	     self->workers_count);

	fs_mkdir(0755, "%s", path);

	// create <base>/<lsn>.incomplete/catalog
	checkpoint_create_catalog(self);

	// run workers
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		checkpoint_worker_run(self, worker);
	}
}

void
checkpoint_wait(Checkpoint* self)
{
	int errors = 0;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		bool error;
		error = checkpoint_worker_wait(worker);
		if (error)
		{
			errors++;
			continue;
		}
	}
	if (errors > 0)
	{
		fs_rmdir("%s/%" PRIu64 ".incomplete", config_directory(), self->lsn);
		error("checkpoint %" PRIu64 ": failed", self->lsn);
	}

	// rename as completed
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%" PRIu64 ".incomplete",
	         config_directory(), self->lsn);
	fs_rename(path, "%s/%" PRIu64, config_directory(), self->lsn);

	// done

	// register checkpoint
	checkpoint_mgr_add(self->mgr, self->lsn);
	var_int_set(&config()->checkpoint, self->lsn);
	info("checkpoint %" PRIu64 ": complete", self->lsn);
}
