
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
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>

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
	{
		auto part = (Part*)index->iface_arg;
		row_free(part->heap, ref->row_prev);
	}
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto row = log_if_rollback(self, op);
	if (op->cmd != CMD_DELETE && row)
	{
		auto index = (Index*)op->iface_arg;
		auto part  = (Part*)index->iface_arg;
		row_free(part->heap, row);
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
		value = *(int32_t*)row_column(row, columns->identity->order);
	else
		value = *(int64_t*)row_column(row, columns->identity->order);
	sequence_sync(self->seq, value);
}

hot void
part_insert(Part* self, Tr* tr, bool replace, Row* row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, &self->id.id_table);

	// update primary index
	op->row_prev = index_replace_by(primary, row);
	if (unlikely(op->row_prev && !replace))
		error("index '%.*s': unique key constraint violation",
		      str_size(&primary->config->name),
		      str_of(&primary->config->name));

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev && !replace))
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
	// get if exists (iterator is openned in both cases)
	auto primary = part_primary(self);
	if (index_upsert(primary, row, it))
	{
		row_free(self->heap, row);
		return true;
	}

	// insert

	// add log record
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, &self->id.id_table);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev))
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
	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL);
	if (! self->unlogged)
		log_persist(&tr->log, &self->id.id_table);

	// update primary index
	op->row_prev = index_replace(primary, row, it);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
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

hot void
part_delete(Part* self, Tr* tr, Iterator* it)
{
	// handle deletes as updates for non in-memory partitions and
	// during compaction
	if (heap_snapshot_has(self->heap))
	{
		auto row = iterator_at(it);
		auto row_delete = row_copy(self->heap, row);
		row_delete->is_delete = true;
		part_update(self, tr, it, row_delete);
		return;
	}

	// add log record
	auto primary = part_primary(self);
	auto row = iterator_at(it);
	auto op = log_row(&tr->log, CMD_DELETE, &log_if, primary, row, NULL);

	// update primary index
	op->row_prev = index_delete(primary, it);
	if (! self->unlogged)
		log_persist(&tr->log, &self->id.id_table);

	// secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_DELETE, &log_if_secondary, index, row, NULL);

		// delete by key
		op->row_prev = index_delete_by(index, row);
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
part_apply(Part* self, Row* row, bool delete)
{
	auto primary = part_primary(self);

	// sync last identity column value during recover
	part_sync_sequence(self, row, index_keys(primary)->columns);

	// update primary index
	if (delete)
		index_delete_by(primary, row);
	else
		index_replace_by(primary, row);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		if (delete)
			index_delete_by(index, row);
		else
			index_replace_by(index, row);
	}
}
