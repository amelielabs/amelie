
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

static inline Row*
rollback(LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	auto part  = (Part*)index->iface_arg;
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
	return op->row;
}

hot static void
log_if_commit(Log* self, LogOp* op)
{
	unused(self);
	auto index = (Index*)op->iface_arg;
	auto part  = (Part*)index->iface_arg;
	auto heap  = part->heap;
	auto row   = op->row;
	if (op->cmd == LOG_DELETE)
	{
		// no clones or versions
		row_free(heap, &part->flats, row);
		return;
	}

	// cleanup version chain starting from head
	if (! row->head)
		return;

	// free older versions related to this timeline
	row_gc(row, heap, &part->flats, op->timeline);

	// last delete in the index
	if (row->deleted && !row_prev_has(row))
	{
		IndexOp io;
		index_op_set(&io, row);
		index_delete(index, &io);
		for (index = index->next; index; index = index->next)
			index_delete(index, &io);
		usage_update(part->arg->memory, io.delta);

		row_free(heap, &part->flats, row);
	}
}

static void
log_if_abort(Log* self, LogOp* op)
{
	unused(self);
	auto row = rollback(op);

	if (op->cmd != LOG_DELETE && row)
	{
		auto index = (Index*)op->iface_arg;
		auto part  = (Part*)index->iface_arg;
		row_free(part->heap, &part->flats, row);
	}

	if (op->row_prev)
	{
		op->row_prev->head = true;

		// unfilter vector columns
		auto index = (Index*)op->iface_arg;
		auto part  = (Part*)index->iface_arg;
		row_filter(&part->flats, op->row_prev, false);
	}
}

hot static void
log_if_secondary_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
	// do nothing
}

static void
log_if_secondary_abort(Log* self, LogOp* op)
{
	unused(self);
	rollback(op);
}

static LogIf log_if =
{
	.commit = log_if_commit,
	.abort  = log_if_abort
};

static LogIf log_if_secondary =
{
	.commit = log_if_secondary_commit,
	.abort  = log_if_secondary_abort
};

static inline void
part_cdc(Part* self, Tr* tr, Timeline* timeline, Row* row, int cdc_cmd)
{
	if (! timeline->rel->subs)
		return;
	cdc_log_add_row(&tr->log.cdc, cdc_cmd, timeline->rel->id,
	                row,
	                &self->flats,
	                self->arg->columns,
	                runtime()->timezone);
}

hot void
part_insert(Part*     self, Tr* tr,
            Timeline* timeline,
            Row*      row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_replace(&tr->log, &log_if, primary, row, timeline);

	IndexOp io;
	index_op_set(&io, row);

	// update primary index
	if (index_replace(primary, &io))
	{
		op->row_prev = io.row_prev;

		// check unique constraint
		if (row_visible(io.row_prev, self->heap, timeline))
			error("index '{str}': unique key constraint violation",
			      &primary->config->name);

		// chain head row
		row_prev_set(row, op->row_prev);
		op->row_prev->head = false;
	}
	row->head = true;

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log, &log_if_secondary, index, row, timeline);
		if (index_replace(index, &io))
		{
			op->row_prev = io.row_prev;
			if (unlikely(row_visible(io.row_prev, self->heap, timeline)))
				error("index '{str}': unique key constraint violation",
				      &index->config->name);
		}
	}

	// capture write
	part_cdc(self, tr, timeline, row, CDC_WRITE);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}

hot bool
part_upsert(Part*     self, Tr* tr, Iterator* it,
            Timeline* timeline,
            Row*      row)
{
	// get if exists (iterator is openned in both cases)
	auto primary = part_primary(self);
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
	row->head = true;

	// add log record
	auto op = log_replace(&tr->log, &log_if, primary, row, timeline);

	// update secondary indexes
	io.it = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log, &log_if_secondary, index, row, timeline);
		if (index_replace(index, &io))
		{
			op->row_prev = io.row_prev;
			if (unlikely(row_visible(io.row_prev, self->heap, timeline)))
				error("index '{str}': unique key constraint violation",
				      &index->config->name);
		}
	}

	// capture write
	part_cdc(self, tr, timeline, row, CDC_WRITE);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
	return false;
}

hot void
part_update(Part*     self, Tr* tr, Iterator* it,
            Timeline* timeline,
            Row*      row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_replace(&tr->log, &log_if, primary, row, timeline);

	// update primary index
	IndexOp io =
	{
		.row      = row,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	index_replace(primary, &io);
	op->row_prev = io.row_prev;
	assert(op->row_prev->head);
	op->row_prev->head = false;

	// chain head row
	row_prev_set(row, op->row_prev);
	row->head = true;

	// filter vector columns
	row_filter(&self->flats, io.row_prev, true);

	// update secondary indexes
	io.it = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_replace(&tr->log,&log_if_secondary, index, row, timeline);

		// replace by key
		if (index_replace(index, &io))
			op->row_prev = io.row_prev;
	}

	// capture
	part_cdc(self, tr, timeline, row, row->deleted? CDC_DELETE: CDC_WRITE);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}

hot void
part_delete(Part* self, Tr* tr, Iterator* it, Timeline* timeline)
{
	auto primary = part_primary(self);

	// add log record
	auto row = iterator_at(it);
	auto op = log_delete(&tr->log, &log_if, primary, row, timeline);

	// update primary index
	IndexOp io =
	{
		.row      = NULL,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	index_delete(primary, &io);
	op->row_prev = io.row_prev;
	op->row_prev->head = false;

	// filter vector columns
	row_filter(&self->flats, op->row_prev, true);

	// secondary indexes
	io.row = row;
	io.it  = NULL;
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_delete(&tr->log, &log_if_secondary, index, row, timeline);

		// delete by key
		if (index_delete(index, &io))
			op->row_prev = io.row_prev;
	}

	// capture delete
	part_cdc(self, tr, timeline, row, CDC_DELETE);

	// ensure write limit
	if (tr->write)
		usage_add(tr->write, 1);

	// ensure memory limit
	usage_add(self->arg->memory, io.delta);
}
