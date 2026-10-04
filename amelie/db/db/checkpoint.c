
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

#if 0
void
checkpoint_init(Checkpoint* self, Catalog* catalog)
{
	self->lsn     = 0;
	self->catalog = catalog;
	self->pid     = -1;
	event_init(&self->on_complete);
	notify_init(&self->notify);
}

void
checkpoint_free(Checkpoint* self)
{
	notify_close(&self->notify);
}

static inline void
checkpoint_worker_on_complete(void* arg)
{
	Event* event = arg;
	event_signal(event);
}

void
checkpoint_begin(Checkpoint* self, uint64_t lsn)
{
	// prepare
	self->lsn = lsn;
	self->pid = -1;
	event_init(&self->on_complete);
	notify_init(&self->notify);
	notify_open(&self->notify, &am_task->poller, checkpoint_worker_on_complete,
	            &self->on_complete);
}

hot static void
checkpoint_heap(Checkpoint* self, Part* part)
{
	// <base>/checkpoint/<lsn>.incomplete/<table_id>.<partition>
	auto rel = part->arg->rel;
	char uuid[UUID_SZ];
	uuid_get(rel->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoint/{u64}.incomplete/{s}.{02d}",
	       state_directory(),
	       self->lsn,
	       uuid,
	       part->config->id);

	auto size = heap_create(part->heap, path);
	info(" {s}.{02d}    ({.2f} MB)",
	     uuid,
	     (int)part->config->id,
	     (double)size / 1024 / 1024);
}

hot static void
checkpoint_flat(Checkpoint* self, Part* part, Flat* flat)
{
	// <base>/checkpoint/<lsn>.incomplete/<table_id>.<partition>.<column>
	auto rel = part->arg->rel;
	char uuid[UUID_SZ];
	uuid_get(rel->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoint/{u64}.incomplete/{s}.{02d}.{02d}",
	       state_directory(),
	       self->lsn,
	       uuid,
	       part->config->id,
	       flat->column->order);

	auto size = flat_create(flat, path);
	info(" {s}.{02d}.{02d} ({.2f} MB)",
	     uuid,
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
checkpoint_table(Checkpoint* self, Table* table)
{
	list_foreach(&table->parts.list)
	{
		auto part = list_at(Part, link);
		checkpoint_part(self, part);
	}
}

static void
checkpoint_main(Checkpoint* self)
{
	// create schema.sql
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete/schema.sql",
	       state_directory(), self->lsn);
	catalog_write(self->catalog, path);

	// create files
	list_foreach(&self->catalog->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type == REL_TABLE)
			checkpoint_table(self, table_of(rel));
	}
}

void
checkpoint_run(Checkpoint* self)
{
	// create <base>/checkpoint/<lsn>.incomplete
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete",
	       state_directory(), self->lsn);

	info("");
	info("checkpoint: checkpoint/{u64}", self->lsn);
	fs_mkdir(0755, "{s}", path);

	// create new process
	self->pid = fork();
	switch (self->pid) {
	case -1:
		error_system();
	case  0:
		break;
	default:
		return;
	}

	// write checkpoint
	auto error = error_catch
	(
		checkpoint_main(self);
	);

	// signal waiter process
	notify_signal(&self->notify);

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
checkpoint_wait_pid(Checkpoint* self)
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
checkpoint_wait(Checkpoint* self)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete",
	       state_directory(), self->lsn);

	// wait for completion
	auto error = checkpoint_wait_pid(self);
	if (error > 0)
	{
		fs_rmdir("{s}", path);
		error("checkpoint: {u64} failed", self->lsn);
	}

	// sync checkpoint dir
	if (opt_int_of(&config()->storage_sync))
		fs_syncdir("{s}", path);

	// sync checkpoint base dir
	if (opt_int_of(&config()->storage_sync))
		fs_syncdir("{s}/checkpoint", state_directory());

	// rename as completed
	fs_rename(path, "{s}/checkpoint/{u64}", state_directory(), self->lsn);

	// done
	info("");
}
#endif

#if 0
typedef struct Checkpoint Checkpoint;

struct Checkpoint
{
	uint64_t lsn;
	Catalog* catalog;
};

void checkpoint_init(Checkpoint*, Catalog*);
void checkpoint_free(Checkpoint*);
void checkpoint_begin(Checkpoint*, uint64_t);
void checkpoint_run(Checkpoint*);
void checkpoint_wait(Checkpoint*);
#endif

#if 0
void
db_checkpoint(Db* self)
{
	uint64_t lsn = state_lsn();
	if (lsn == state_checkpoint())
		return;

	// one checkpoint, create index or backup at a time
	auto checkpoint_lock = lock_system(REL_CHECKPOINT, LOCK_EXCLUSIVE);
	defer(unlock, checkpoint_lock);

	// take exclusive catalog lock
	auto catalog_lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	// force commit pending prepared transactions
	list_foreach(&self->catalog.rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type == REL_TABLE)
			table_sync(table_of(rel));
	}

	// prepare and start workers
	Checkpoint checkpoint;
	checkpoint_init(&checkpoint, &self->catalog);
	defer(checkpoint_free, &checkpoint);
	auto on_error = error_catch
	(
		checkpoint_begin(&checkpoint, lsn);
		checkpoint_run(&checkpoint);
	);

	// unlock catalog
	unlock(catalog_lock);

	if (on_error)
		rethrow();

	// wait for completion
	on_error = error_catch (
		checkpoint_wait(&checkpoint);
	);
	if (on_error)
		rethrow();

	// set checkpoint
	checkpoints_add(&self->checkpoints, lsn);
}
#endif

typedef struct CheckpointPage CheckpointPage;
typedef struct Checkpoint     Checkpoint;

struct CheckpointPage
{
	Id    id;
	Page* cow;
};

struct Checkpoint
{
	uint64_t lsn;
	Buf      list;
	Buf      schema;
};

static inline void
checkpoint_init(Checkpoint* self, uint64_t lsn)
{
	self->lsn = lsn;
	buf_init(&self->list);
	buf_init(&self->schema);
}

static inline void
checkpoint_free(Checkpoint* self)
{
	buf_free(&self->list);
	buf_free(&self->schema);
}

static inline void
checkpoint_add(Checkpoint* self, Id* id, Page* cow)
{
	auto ref = (CheckpointPage*)buf_emplace(&self->list, sizeof(CheckpointPage));
	memcpy(&ref->id, id, sizeof(*id));
	ref->cow = cow;
}

void
checkpoint(Checkpoints* checkpoints, Catalog* catalog)
{
	if (state_lsn() == state_checkpoint())
		return;

	// one checkpoint, create index or backup at a time
	auto checkpoint_lock = lock_system(REL_CHECKPOINT, LOCK_EXCLUSIVE);
	defer(unlock, checkpoint_lock);

	// take exclusive catalog lock
	auto catalog_lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	Checkpoint cp;
	checkpoint_init(&cp, state_lsn());
	defer(checkpoint_free, &cp);

	// create schema.sql content
	describe_catalog(catalog, &cp.schema);

#if 0
	// collect changed pages
	list_foreach(&self->catalog.rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type == REL_TABLE)
			table_sync(table_of(rel));

	}
#endif

	(void)checkpoints;
	(void)catalog;
}
