
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>

void
save_init(Save* self, Catalog* catalog)
{
	self->lsn           = 0;
	self->workers       = NULL;
	self->workers_count = 0;
	self->catalog       = catalog;
}

void
save_free(Save* self)
{
	if (self->workers)
	{
		for (int i = 0; i < self->workers_count; i++)
		{
			auto worker = &self->workers[i];
			notify_close(&worker->notify);
		}
		am_free(self->workers);
	}
	self->workers = NULL;
	self->workers_count = 0;
}

static inline void
save_worker_on_complete(void* arg)
{
	Event* event = arg;
	event_signal(event);
}

void
save_begin(Save* self, uint64_t lsn, int workers)
{
	// prepare workers
	self->lsn = lsn;
	self->workers_count = workers;
	self->workers = am_malloc(sizeof(SaveWorker) * workers);
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		worker->pid        = -1;
		worker->list_count = 0;
		event_init(&worker->on_complete);
		notify_init(&worker->notify);
		notify_open(&worker->notify, &am_task->poller,
		            save_worker_on_complete,
		            &worker->on_complete);
		list_init(&worker->list);
	}

	// distribute partitions between workers
	auto rr = 0;
	list_foreach(&self->catalog->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;
		auto table = table_of(rel);
		list_foreach(&table->part_mgr.list)
		{
			auto part = list_at(Part, link);
			if (rr == self->workers_count)
				rr = 0;
			auto order  = rr++;
			auto worker = &self->workers[order];
			list_init(&part->link_cp);
			list_append(&worker->list, &part->link_cp);
			worker->list_count++;
		}
	}
}

hot static void
save_part(Save* self, Part* part)
{
	// <base>/checkpoints/<lsn>.incomplete/<partition_id>
	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoints/{u64}.incomplete/{u64}",
	       state_directory(),
	       self->lsn,
	       part->config->id);
	auto size = heap_create(part->heap, path);
	info(" {u64}/{u64} ({.2f} MiB)",
	     self->lsn,
	     part->config->id,
	     (double)size / 1024 / 1024);
}

static void
save_worker_run(Save* self, SaveWorker* worker)
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
	auto error = error_catch(
		list_foreach(&worker->list)
			save_part(self, list_at(Part, link_cp));
	);

	// signal waiter process
	notify_signal(&worker->notify);

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
save_worker_wait(SaveWorker* self)
{
	event_wait(&self->on_complete, -1);

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

void
save_run(Save* self)
{
	// create <base>/checkpoints/<lsn>.incomplete
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoints/{u64}.incomplete",
	       state_directory(), self->lsn);

	info("");
	info("checkpoint: checkpoints/{u64} / (using {d} workers)",
	     self->lsn, self->workers_count);
	fs_mkdir(0755, "{s}", path);

	// create <base>/checkpoints/<lsn>.incomplete/catalog.json
	format(path, sizeof(path), "{s}/checkpoints/{u64}.incomplete/catalog.json",
	       state_directory(), self->lsn);
	catalog_write(self->catalog, path);

	// run workers
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		save_worker_run(self, worker);
	}
}

void
save_wait(Save* self)
{
	int errors = 0;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		bool error;
		error = save_worker_wait(worker);
		if (error)
		{
			errors++;
			continue;
		}
	}
	if (errors > 0)
	{
		fs_rmdir("{s}/checkpoints/{u64}.incomplete",
		         state_directory(), self->lsn);
		error("checkpoint: {u64} failed", self->lsn);
	}

	// rename as completed
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoints/{u64}.incomplete",
	       state_directory(), self->lsn);
	fs_rename(path, "{s}/checkpoints/{u64}", state_directory(), self->lsn);

	// done
	info("");
}
