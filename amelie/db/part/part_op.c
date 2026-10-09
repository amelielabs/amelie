
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

static inline void
rollback(LogOp* op, Part* part)
{
	auto index = (Index*)op->iface_arg;
	IndexOp io;
	if (op->row_prev)
	{
		index_op_set(&io, op->row_prev);
		index_replace(index, &io);
		usage_update(part->arg->memory, io.delta);
	} else
	if (op->row)
	{
		index_op_set(&io, op->row);
		index_delete(index, &io);
		usage_update(part->arg->memory, io.delta);
	}
}

hot static void
primary_if_commit(Log* self, LogOp* op)
{
	Part* part = self->arg;
	auto heap  = part->heap;

	// mark row as commited
	auto row = op->row;
	row->commited = true;
	if (op->cmd == LOG_DELETE)
	{
		row_free(heap, &part->flats, row);
		return;
	}

	if (op->row_prev)
		row_free(heap, &part->flats, op->row_prev);
}

static void
primary_if_abort(Log* self, LogOp* op)
{
	Part* part = self->arg;
	auto index = (Index*)op->iface_arg;
	if (index)
		rollback(op, self->arg);

	if (op->cmd != LOG_DELETE && op->row)
		row_free(part->heap, &part->flats, op->row);

	// unfilter vector columns
	if (op->row_prev)
		row_filter(&part->flats, op->row_prev, false);
}

hot static void
secondary_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
	// do nothing
}

static void
secondary_if_abort(Log* self, LogOp* op)
{
	rollback(op, self->arg);
}

static LogIf primary_if =
{
	.commit = primary_if_commit,
	.abort  = primary_if_abort
};

static LogIf secondary_if =
{
	.commit = secondary_if_commit,
	.abort  = secondary_if_abort
};

hot void
part_insert(Part* self, Tr* tr, Row* row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_replace(&tr->log, &primary_if, primary, row);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	if (! primary)
		return;

	// update primary index
	IndexOp io;
	index_op_set(&io, row);
	if (index_replace(primary, &io))
	{
		op->row_prev = io.row_prev;
		error("index '{str}': unique key constraint violation",
		      &primary->config->name);
	}

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log, &secondary_if, index, row);
		if (index_replace(index, &io))
		{
			op->row_prev = io.row_prev;
			error("index '{str}': unique key constraint violation",
				  &index->config->name);
		}
	}

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}

hot bool
part_upsert(Part* self, Tr* tr, Iterator* it, Row* row)
{
	// ensure primary key is defined
	auto primary = part_primary(self);
	if (! primary)
		error("upsert: requires primary index");

	// get if exists (iterator is openned in both cases)
	IndexOp io =
	{
		.row      = row,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	if (index_upsert(primary, &io))
	{
		assert(iterator_at(it));
		row_free(self->heap, &self->flats, row);

		// ensure memory limit
		usage_add(self->arg->memory, io.delta);
		return true;
	}

	// insert

	// add log record
	auto op = log_replace(&tr->log, &primary_if, primary, row);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	// update secondary indexes
	io.it = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log, &secondary_if, index, row);
		if (index_replace(index, &io))
		{
			op->row_prev = io.row_prev;
			error("index '{str}': unique key constraint violation",
			      &index->config->name);
		}
	}

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
	return false;
}

hot void
part_update(Part* self, Tr* tr, Iterator* it, Row* row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_replace(&tr->log, &primary_if, primary, row);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	op->row_prev = iterator_at(it);

	// filter vector columns
	row_filter(&self->flats, op->row_prev, true);

	if (! primary)
		return;

	// update primary index
	IndexOp io =
	{
		.row      = row,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	index_replace(primary, &io);

	// update secondary indexes
	io.it = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log, &secondary_if, index, row);

		// replace by key
		if (index_replace(index, &io))
			op->row_prev = io.row_prev;
	}

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}

hot void
part_delete(Part* self, Tr* tr, Iterator* it)
{
	// add log record
	auto primary = part_primary(self);
	auto row = iterator_at(it);
	auto op = log_delete(&tr->log, &primary_if, primary, row);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	op->row_prev = row;

	// filter vector columns
	row_filter(&self->flats, op->row_prev, true);

	if (! primary)
		return;

	// update primary index
	IndexOp io =
	{
		.row      = NULL,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	index_delete(primary, &io);

	// secondary indexes
	io.row = row;
	io.it  = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_delete(&tr->log, &secondary_if, index, row);

		// delete by key
		if (index_delete(index, &io))
			op->row_prev = io.row_prev;
	}

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}
