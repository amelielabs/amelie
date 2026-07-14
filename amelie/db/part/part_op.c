
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
	if (op->row_prev)
		index_replace_by(index, op->row_prev);
	else
	if (op->row)
		index_delete_by(index, op->row);
	return op->row;
}

hot static void
log_if_commit(Log* self, LogOp* op)
{
	unused(self);
	auto index = (Index*)op->iface_arg;
	auto part  = (Part*)index->iface_arg;
	auto heap  = part->heap;
	if (op->cmd == LOG_DELETE)
	{
		// no clones or versions
		row_free(heap, &part->flats, op->row);
		return;
	}

	// mark row head and cleanup versions
	row_seal_and_gc(op->row, heap, &part->flats, op->timeline);

	// last delete in the chain
	if (op->row->deleted && !row_prev(op->row, heap))
	{
		index_delete_by(index, op->row);
		for (index = index->next; index; index = index->next)
			index_delete_by(index, op->row);

		row_free(heap, &part->flats, op->row);
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
part_cdc(Part* self, Tr* tr, Timeline* timeline, Index* primary)
{
	unused(self);
	if (! timeline->rel->subs)
		return;
	auto last = log_last(&tr->log);
	log_cdc(&tr->log, last->cmd, timeline->rel->id, last->row,
	        index_keys(primary)->columns,
	        runtime()->timezone);
}

hot void
part_insert(Part*     self, Tr* tr,
            Timeline* timeline,
            Row*      row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_dml(&tr->log, LOG_REPLACE, &log_if, primary, row, NULL, timeline);

	// handle cdc
	part_cdc(self, tr, timeline, primary);

	// update primary index
	op->row_prev = index_replace_by(primary, row);
	if (op->row_prev)
	{
		// check unique constraint
		if (row_visible(op->row_prev, self->heap, timeline))
			error("index '{str}': unique key constraint violation",
			      &primary->config->name);

		// chain head row
		row_prev_set(row, op->row_prev);
	}

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_dml(&tr->log, LOG_REPLACE, &log_if_secondary, index, row, NULL, timeline);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev))
			if (row_visible(op->row_prev, self->heap, timeline))
				error("index '{str}': unique key constraint violation",
				      &index->config->name);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot bool
part_upsert(Part*     self, Tr* tr, Iterator* it,
            Timeline* timeline,
            Row*      row)
{
	// get if exists (iterator is openned in both cases)
	auto primary = part_primary(self);
	if (index_upsert(primary, row, it))
	{
		row_free(self->heap, &self->flats, row);
		return true;
	}

	// insert

	// add log record
	auto op = log_dml(&tr->log, LOG_REPLACE, &log_if, primary, row, NULL, timeline);

	// handle cdc
	part_cdc(self, tr, timeline, primary);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_dml(&tr->log, LOG_REPLACE, &log_if_secondary, index, row, NULL, timeline);
		op->row_prev = index_replace_by(index, row);

		if (unlikely(op->row_prev))
			if (row_visible(op->row_prev, self->heap, timeline))
				error("index '{str}': unique key constraint violation",
				      &index->config->name);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	return false;
}

hot void
part_update(Part*     self, Tr* tr, Iterator* it,
            Timeline* timeline,
            Row*      row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_dml(&tr->log, LOG_REPLACE, &log_if, primary, row, NULL, timeline);

	// handle cdc
	part_cdc(self, tr, timeline, primary);

	// update primary index
	op->row_prev = index_replace(primary, row, it);

	// chain head row
	row_prev_set(row, op->row_prev);

	// filter vector columns
	row_filter(&self->flats, op->row_prev, true);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_dml(&tr->log, LOG_REPLACE, &log_if_secondary, index, row, NULL, timeline);

		// find and replace existing secondary row (keys are not updated)
		auto index_it = index_iterator(index);
		defer(iterator_close, index_it);
		iterator_open(index_it, self->heap, timeline, row);
		op->row_prev = index_replace(index, row, index_it);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot void
part_delete(Part* self, Tr* tr, Iterator* it, Timeline* timeline)
{
	auto primary = part_primary(self);

	// handle delete as update to support clonning
	auto arg = self->arg;
	if (arg->timelines->list_count > 0)
	{
		auto row = iterator_at(it);
		row = row_visible(row, self->heap, timeline);
		if (! row)
			return;

		auto columns = index_keys(primary)->columns;
		auto row_delete = row_copykey(self->heap, row, columns);
		row_delete->deleted  = true;
		row_delete->main     = timeline->main;
		row_delete->timeline = timeline->timeline;

		part_update(self, tr, it, timeline, row_delete);
		return;
	}

	// add log record
	auto row = iterator_at(it);
	auto op = log_dml(&tr->log, LOG_DELETE, &log_if, primary, row, NULL, timeline);

	// handle cdc
	part_cdc(self, tr, timeline, primary);

	// update primary index
	op->row_prev = index_delete(primary, it);

	// filter vector columns
	row_filter(&self->flats, op->row_prev, true);

	// secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_dml(&tr->log, LOG_DELETE, &log_if_secondary, index, row, NULL, timeline);

		// delete by key
		op->row_prev = index_delete_by(index, row);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot void
part_delete_by(Part* self, Tr* tr, Timeline* timeline, Row* row)
{
	// note: transaction memory limit is not ensured here
	// since this operation is used for recovery
	auto primary = part_primary(self);
	auto it = index_iterator(primary);
	defer(iterator_close, it);
	if (unlikely(! iterator_open(it, self->heap, timeline, row)))
		error("delete by key does not match");
	part_delete(self, tr, it, timeline);
}

void
part_follow(Part* self, Row* row, Columns* columns)
{
	// use first identity column to sync the sequence
	if (! columns->identity)
		return;
	int64_t value;
	if (columns->identity->size == 4)
		value = *(int32_t*)row_column(row, columns->identity);
	else
		value = *(int64_t*)row_column(row, columns->identity);
	sequence_sync(self->arg->seq, value);
}
