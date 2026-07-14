
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
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>

void
checkpoint_init(Checkpoint* self, Catalog* catalog)
{
	self->lsn           = 0;
	self->workers       = NULL;
	self->workers_count = 0;
	self->catalog       = catalog;
}

void
checkpoint_free(Checkpoint* self)
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
checkpoint_worker_on_complete(void* arg)
{
	Event* event = arg;
	event_signal(event);
}

void
checkpoint_begin(Checkpoint* self, uint64_t lsn, int workers)
{
	// ensure workers are defined
	if (workers < 1)
		error("checkpoint: 1 or more workers required");

	// prepare workers
	self->lsn = lsn;
	self->workers_count = workers;
	self->workers = am_malloc(sizeof(CheckpointWorker) * workers);
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		worker->cdc        = false;
		worker->pid        = -1;
		worker->list_count = 0;
		event_init(&worker->on_complete);
		notify_init(&worker->notify);
		notify_open(&worker->notify, &am_task->poller,
		            checkpoint_worker_on_complete,
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
		list_foreach(&table->parts.list)
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

	// set cdc dump to a first worker
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (! worker->list_count)
			continue;
		worker->cdc = true;
		break;
	}
}

hot static void
checkpoint_heap(Checkpoint* self, Part* part)
{
	// <base>/checkpoint/<lsn>.incomplete/<partition_id>
	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoint/{u64}.incomplete/{u64}",
	       state_directory(),
	       self->lsn,
	       part->config->id);

	auto size = heap_create(part->heap, path);
	info(" {u64}/{u64} ({.2f} MiB)",
	     self->lsn,
	     part->config->id,
	     (double)size / 1024 / 1024);
}

hot static void
checkpoint_flat(Checkpoint* self, Part* part, Flat* flat)
{
	// <base>/checkpoint/<lsn>.incomplete/<partition_id>.<flat_id>
	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoint/{u64}.incomplete/{u64}.{d}",
	       state_directory(),
	       self->lsn,
	       part->config->id,
	       flat->column->order);

	auto size = flat_create(flat, path);
	info(" {u64}/{u64}.{d} ({.2f} MiB)",
	     self->lsn,
	     part->config->id,
	     flat->column->order,
	     (double)size / 1024 / 1024);
}

hot static void
checkpoint_part(Checkpoint* self, Part* part)
{
	checkpoint_heap(self, part);

	auto primary = part_primary(part);
	auto columns = index_keys(primary)->columns;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->size_flat)
			continue;
		auto flat = flats_at(&part->flats, column);
		checkpoint_flat(self, part, flat);
	}
}

hot static void
checkpoint_cdc(Checkpoint* self)
{
	// <base>/checkpoint/<lsn>.incomplete/cdc
	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoint/{u64}.incomplete/cdc",
	       state_directory(),
	       self->lsn);

	auto size = cdc_create(self->catalog->cdc, path);
	info(" {u64}/cdc ({.2f} MiB)",
	     self->lsn,
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

	auto error = error_catch
	(
		// create partition files
		list_foreach(&worker->list)
			checkpoint_part(self, list_at(Part, link_cp));

		// create cdc file
		if (worker->cdc)
			checkpoint_cdc(self);
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
checkpoint_worker_wait(CheckpointWorker* self)
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
checkpoint_run(Checkpoint* self)
{
	// create <base>/checkpoint/<lsn>.incomplete
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete",
	       state_directory(), self->lsn);

	info("");
	info("checkpoint: checkpoint/{u64} (using {d} workers)",
	     self->lsn, self->workers_count);
	fs_mkdir(0755, "{s}", path);

	// create <base>/checkpoint/<lsn>.incomplete/catalog.json
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete/catalog.json",
	       state_directory(), self->lsn);
	catalog_write(self->catalog, path);

	// run workers
	auto active = 0;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		checkpoint_worker_run(self, worker);
		active++;
	}

	// create cdc file (no relations)
	if (! active)
		checkpoint_cdc(self);
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
		fs_rmdir("{s}/checkpoint/{u64}.incomplete",
		         state_directory(), self->lsn);
		error("checkpoint: {u64} failed", self->lsn);
	}

	// rename as completed
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete",
	       state_directory(), self->lsn);
	fs_rename(path, "{s}/checkpoint/{u64}", state_directory(), self->lsn);

	// done
	info("");
}
