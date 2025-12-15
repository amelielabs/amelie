
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

static Row*
log_if_rollback(Log* self, LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	auto ref = log_row_of(self, op);
	if (ref->row_prev)
		index_replace_by(index, ref->row_prev);
	else
	if (ref->row)
		index_delete_by(index, ref->row);
	return ref->row;
}

hot static void
log_if_commit(Log* self, LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	auto ref = log_row_of(self, op);
	if (ref->row_prev)
		row_free(index->heap, ref->row_prev);
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto row = log_if_rollback(self, op);
	if (row)
	{
		auto index = (Index*)op->iface_arg;
		row_free(index->heap, row);
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
	log_if_rollback(self, op);
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
part_sync_sequence(Part* self, Row* row, Columns* columns)
{
	// use first identity column to sync the sequence
	if (! columns->identity)
		return;
	int64_t value;
	if (columns->identity->type_size == 4)
		value = *(int32_t*)row_at(row, columns->identity->order);
	else
		value = *(int64_t*)row_at(row, columns->identity->order);
	sequence_sync(self->seq, value);
}

hot void
part_insert(Part* self, Tr* tr, bool replace, Row* row)
{
	// set row and heap tsn
	row_follow_tsn(row, &self->heap, tr->tsn);

	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, self->config->id);

	// update primary index
	op->row_prev = index_replace_by(primary, row);
	if (op->row_prev)
	{
		// insert on non deleted row
		if (!replace && !op->row_prev->is_deleted)
			error("index '%.*s': unique key constraint violation",
			      str_size(&primary->config->name),
			      str_of(&primary->config->name));

		// validate serial order, update prev row tsn
		row_write(tr, op->row_prev);
	}

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev && !replace && !op->row_prev->is_deleted))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}

	// sync last identity column value during recover
	if (replace)
		part_sync_sequence(self, row, index_keys(primary)->columns);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot bool
part_upsert(Part* self, Tr* tr, Iterator* it, Row* row)
{
	// set row and heap tsn
	row_follow_tsn(row, &self->heap, tr->tsn);

	// insert or get (iterator is openned in both cases)
	auto primary = part_primary(self);
	Row* head = NULL;
	if (index_upsert(primary, row, it))
	{
		head = iterator_at(it);
		if (! head->is_deleted)
		{
			row_free(&self->heap, row);
			row_write(tr, head);
			return true;
		}

		// upsert on deleted row

		// (free row in case of write error)
		if (unlikely(row_tsn(head) > tr->tsn))
			row_free(&self->heap, row);
		row_write(tr, head);

		// force replace index head
		index_replace(primary, row, it);
	}

	// insert or replace delete

	// add log record
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, head);
	if (! self->unlogged)
		log_persist(&tr->log, self->config->id);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);

		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev && !op->row_prev->is_deleted))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	return false;
}

hot void
part_update(Part* self, Tr* tr, Iterator* it, Row* row)
{
	assert(! iterator_at(it)->is_deleted);

	// set row and heap tsn
	row_follow_tsn(row, &self->heap, tr->tsn);

	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, self->config->id);

	// update primary index
	op->row_prev = index_replace(primary, row, it);

	// validate serial order, update prev row tsn
	row_write(tr, op->row_prev);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);

		// add log record
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL);

		// find and replace existing secondary row (keys are not updated)
		auto index_it = index_iterator(index);
		defer(iterator_close, index_it);
		iterator_open(index_it, row);
		op->row_prev = index_replace(index, row, index_it);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot static void
log_if_delete_commit(Log* self, LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	auto row   = log_row_of(self, op)->row_prev;

	// already updated by next transaction
	auto tsn = row_tsn(row);
	if (tsn != self->tsn)
		return;

	index_delete_by(index, row);
}

static void
log_if_delete_abort(Log* self, LogOp* op)
{
	auto row = log_row_of(self, op)->row_prev;
	row->is_deleted = false;
}

static LogIf log_if_delete =
{
	.commit = log_if_delete_commit,
	.abort  = log_if_delete_abort
};

hot void
part_delete(Part* self, Tr* tr, Iterator* it)
{
	auto row = iterator_at(it);
	assert(! row->is_deleted);

	// set row and heap tsn
	row_write(tr, row);
	heap_follow_tsn(&self->heap, tr->tsn);

	// mark row as deleted
	row->is_deleted = true;

	// add log record
	auto primary = part_primary(self);
	log_row(&tr->log, CMD_DELETE, &log_if_delete, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, self->config->id);

	// secondary indexes (no modifications at this point)
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		log_row(&tr->log, CMD_DELETE, &log_if_delete, index, row, NULL);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot void
part_delete_by(Part* self, Tr* tr, Row* row)
{
	// note: transaction memory limit is not ensured here
	// since this operation is used for recovery
	auto primary = part_primary(self);
	auto it = index_iterator(primary);
	defer(iterator_close, it);
	if (unlikely(! iterator_open(it, row)))
		error("delete by key does not match");
	part_delete(self, tr, it);
}

hot void
part_ingest_secondary(Part* self, Row* row)
{
	// update secondary indexes
	auto primary = part_primary(self);
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		index_replace_by(index, row);
	}
}

hot void
part_ingest(Part* self, Row* row)
{
	auto primary = part_primary(self);

	// sync last identity column value during recover
	part_sync_sequence(self, row, index_keys(primary)->columns);

	// update primary index
	index_replace_by(primary, row);

	// secondary indexes
	part_ingest_secondary(self, row);
}
