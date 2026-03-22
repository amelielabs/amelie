
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
	auto index = (Index*)op->iface_arg;
	auto part  = (Part*)index->iface_arg;
	auto heap  = part->heap;
	if (op->cmd == CMD_DELETE)
	{
		// no branches or versions
		row_free(heap, op->row);
		return;
	}

	// cleanup row chain
	auto tr = container_of(self, Tr, log);
	row_gc(op->row, heap, op->branch, tr->id);

	// last delete in the chain
	if (op->row->is_delete && !row_prev(op->row, heap, heap->shadow))
	{
		index_delete_by(index, op->row);
		for (index = index->next; index; index = index->next)
			index_delete_by(index, op->row);

		row_free(heap, op->row);
	}
}

static void
log_if_abort(Log* self, LogOp* op)
{
	unused(self);
	auto row = rollback(op);
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

hot void
part_insert(Part*   self, Tr* tr, bool replace,
            Branch* branch,
            Row*    row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL, branch);
	if (! self->arg->unlogged)
		log_persist(&tr->log, self->arg->id_table);

	// update primary index
	op->row_prev = index_replace_by(primary, row);
	if (op->row_prev)
	{
		// check unique constraint
		if (!replace && row_visible(op->row_prev, self->heap, branch))
			error("index '%.*s': unique key constraint violation",
			      str_size(&primary->config->name),
			      str_of(&primary->config->name));

		// chain head row
		row_prev_set(row, op->row_prev);
	}

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL, branch);
		op->row_prev = index_replace_by(index, row);
		if (unlikely(op->row_prev && !replace))
			if (row_visible(op->row_prev, self->heap, branch))
				error("index '%.*s': unique key constraint violation",
				      str_size(&index->config->name),
				      str_of(&index->config->name));
	}

	// sync last identity column value during recover
	if (replace)
		part_follow(self, row, index_keys(primary)->columns);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot bool
part_upsert(Part* self, Tr* tr, Iterator* it, Branch* branch, Row* row)
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
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL, branch);
	if (! self->arg->unlogged)
		log_persist(&tr->log, self->arg->id_table);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL, branch);
		op->row_prev = index_replace_by(index, row);

		if (unlikely(op->row_prev))
			if (row_visible(op->row_prev, self->heap, branch))
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
part_update(Part* self, Tr* tr, Iterator* it, Branch* branch, Row* row)
{
	// add log record
	auto primary = part_primary(self);
	auto op = log_row(&tr->log, CMD_REPLACE, &log_if, primary, row, NULL, branch);
	if (! self->arg->unlogged)
		log_persist(&tr->log, self->arg->id_table);

	// update primary index
	op->row_prev = index_replace(primary, row, it);

	// chain head row
	row_prev_set(row, op->row_prev);

	// update secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_REPLACE, &log_if_secondary, index, row, NULL, branch);

		// find and replace existing secondary row (keys are not updated)
		auto index_it = index_iterator(index);
		defer(iterator_close, index_it);
		iterator_open(index_it, self->heap, branch, row);
		op->row_prev = index_replace(index, row, index_it);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot void
part_delete(Part* self, Tr* tr, Iterator* it, Branch* branch)
{
	// handle delete as update to support branching
	if (self->arg->config->branches.list_count > 1)
	{
		auto row = iterator_at(it);
		row = row_visible(row, self->heap, branch);
		if (! row)
			return;

		// todo: copy key
		auto row_delete = row_copy(self->heap, row);
		row_delete->is_delete = true;
		row_delete->tsn       = tr->id;
		row_delete->branch    = branch->id;

		part_update(self, tr, it, branch, row_delete);
		return;
	}

	// add log record
	auto primary = part_primary(self);
	auto row = iterator_at(it);
	auto op = log_row(&tr->log, CMD_DELETE, &log_if, primary, row, NULL, branch);

	// update primary index
	op->row_prev = index_delete(primary, it);
	if (! self->arg->unlogged)
		log_persist(&tr->log, self->arg->id_table);

	// secondary indexes
	for (auto index = primary->next; index; index = index->next)
	{
		// add log record (not persisted)
		op = log_row(&tr->log, CMD_DELETE, &log_if_secondary, index, row, NULL, branch);

		// delete by key
		op->row_prev = index_delete_by(index, row);
	}

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);
}

hot void
part_delete_by(Part* self, Tr* tr, Branch* branch, Row* row)
{
	// note: transaction memory limit is not ensured here
	// since this operation is used for recovery
	auto primary = part_primary(self);
	auto it = index_iterator(primary);
	defer(iterator_close, it);
	if (unlikely(! iterator_open(it, self->heap, branch, row)))
		error("delete by key does not match");
	part_delete(self, tr, it, branch);
}

void
part_follow(Part* self, Row* row, Columns* columns)
{
	// use first identity column to sync the sequence
	if (! columns->identity)
		return;
	int64_t value;
	if (columns->identity->type_size == 4)
		value = *(int32_t*)row_column(row, columns->identity);
	else
		value = *(int64_t*)row_column(row, columns->identity);
	sequence_sync(self->arg->seq, value);
}

hot bool
part_apply(Part* self, Index* secondary)
{
	// complete heap snapshot, copy new rows to the main heap,
	// update indexes and row chains

	// snapshot complete
	heap_snapshot(self->heap, NULL, false);

	// create primary index iterator for upsert
	auto primary   = part_primary(self);
	auto it_upsert = index_iterator(primary);

	// create secondaryindex iterator for upsert
	bool pass = true;
	Iterator* it_upsert_secondary = NULL;
	if (secondary)
		it_upsert_secondary = index_iterator(secondary);

	// apply shadow updates
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, self->heap_shadow, NULL);
	for (; heap_iterator_has(&it); heap_iterator_next(&it))
	{
		auto chunk = heap_iterator_at_chunk(&it);
		if (chunk->is_shadow_free)
		{
			// delayed heap removal
			row_free(self->heap, *(Row**)chunk->data);
			continue;
		}

		// copy row
		auto row  = heap_iterator_at(&it);
		auto copy = row_copy(self->heap, row);

		// get index head
		index_upsert(primary, copy, it_upsert);

		// row is head
		auto head = iterator_at(it_upsert);
		assert(head);
		if (row == head)
		{
			// update indexes to set the copy
			index_replace(primary, copy, it_upsert);
			for (auto index = primary->next; index; index = index->next)
				index_replace_by(index, copy);
		} else
		{
			// find prev of the row
			while (head)
			{
				auto prev = row_prev(head, self->heap, self->heap_shadow);
				if (prev == row)
					break;
				head = prev;
			}

			// set prev->next = copy
			assert(head);
			row_prev_set(head, copy);
		}

		// set copy->prev = row->prev
		auto prev = row_prev(row, self->heap, self->heap_shadow);
		if (prev)
			row_prev_set(copy, prev);

		// update new secondary index
		if (secondary)
		{
			// get index head
			if (! index_upsert(secondary, copy, it_upsert_secondary))
				continue;
			// check unique constraint violation
			auto head = iterator_at(it_upsert_secondary);
			if (row_unique(head, self->heap, self->heap_shadow))
			{
				pass = false;
				break;
			}
		}
	}

	iterator_close(it_upsert);
	if (it_upsert_secondary)
		iterator_close(it_upsert_secondary);
	return pass;
}
