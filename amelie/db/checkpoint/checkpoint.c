
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>

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
			if (worker->eventfd != -1)
				close(worker->eventfd);
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
		worker->pid        = -1;
		worker->eventfd    = -1;
		worker->list_count = 0;
		list_init(&worker->list);
	}

	// prepare eventfd
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		worker->eventfd = eventfd(0, 0);
		if (worker->eventfd == -1)
			error_system();
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

hot static void
checkpoint_part(Checkpoint* self, Part* part)
{
	// <base>/<lsn>.incomplete/<partition_id>
	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/checkpoints/%" PRIu64 ".incomplete/%" PRIu64,
	         state_directory(),
	         self->lsn,
	         part->config->id);
	auto size = heap_file_write(&part->heap, path);
	info("checkpoints/%" PRIu64 "/%" PRIu64 " (%.2f MiB)",
	     self->lsn,
	     part->config->id,
	     (double)size / 1024 / 1024);
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

	// reinit io_uring after fork
	auto io  = &am_task->io;
	auto bus = &am_task->bus;
	bus_free(bus);
	io_free(io);
	io_init(io);
	io_create(io);
	bus_init(bus, io);

	// create snapshots
	auto error = error_catch(
		list_foreach(&worker->list)
			checkpoint_part(self, list_at(Part, link_cp));
	);

	// done
	uint64_t value = 1;
	write(worker->eventfd, &value, sizeof(value));

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

#if 0
static bool
checkpoint_worker_wait(CheckpointWorker* self)
{
	// wait for the completion
	siginfo_t si;
	memset(&si, 0, sizeof(si));
	int rc = io_waitid(P_PID, self->pid, &si, WEXITED);
	if (rc == -1)
		error_system();
	bool is_failed = si.si_code != CLD_EXITED || si.si_status != EXIT_SUCCESS;
	return is_failed;
}
#endif

static bool
checkpoint_worker_wait(CheckpointWorker* self)
{
	// wait for completion
	uint64_t value;
	auto rc = io_pread(self->eventfd, &value, sizeof(value), 0);
	if (rc == -1)
		error_system();

	int status = 0;
	rc = waitpid(self->pid, &status, 0);
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
	// create <base>/<lsn>.incomplete/catalog.json
	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/checkpoints/%" PRIu64 ".incomplete/catalog.json",
	         state_directory(), self->lsn);

	// convert catalog to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = self->catalog->start;
	json_export_pretty(&text, runtime()->timezone, &pos);

	// create config file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, &text);
}

void
checkpoint_run(Checkpoint* self)
{
	// create <base>/<lsn>.incomplete
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/checkpoints/%" PRIu64 ".incomplete",
	         state_directory(), self->lsn);

	info("");
	info("⟶ checkpoint %" PRIu64 " (using %d workers)",
	     self->lsn, self->workers_count);
	fs_mkdir(0755, "%s", path);

	// create <base>/<lsn>.incomplete/catalog.json
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
		fs_rmdir("%s/checkpoints/%" PRIu64 ".incomplete",
		         state_directory(), self->lsn);
		error("%" PRIu64 " failed", self->lsn);
	}

	// rename as completed
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/checkpoints/%" PRIu64 ".incomplete",
	         state_directory(), self->lsn);
	fs_rename(path, "%s/checkpoints/%" PRIu64, state_directory(), self->lsn);

	// done

	// register checkpoint
	checkpoint_mgr_add(self->mgr, self->lsn);
	opt_int_set(&state()->checkpoint, self->lsn);
	info("complete");
}
