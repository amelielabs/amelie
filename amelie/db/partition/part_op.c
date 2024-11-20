
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_value.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

static Row*
log_if_rollback(Log* self, LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	auto ref = log_row_of(self, op);
	if (ref->row_prev)
		index_set(index, ref->row_prev);
	else
	if (ref->row)
		index_delete_by(index, ref->row);
	return ref->row;
}

hot static void
log_if_commit(Log* self, LogOp* op)
{
	auto ref = log_row_of(self, op);
	if (ref->row_prev)
		row_free(ref->row_prev);
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto row = log_if_rollback(self, op);
	if (op->cmd != LOG_DELETE && row)
		row_free(row);
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

#if 0
static inline void
part_sync_serial(Part* self, uint8_t* pos_serial)
{
	int64_t value;
	data_read_integer(&pos_serial, &value);
	serial_sync(self->serial, value);
}
#endif

hot void
part_insert(Part* self, Tr* tr, bool recover, Row* row)
{
	auto primary = part_primary(self);
	auto keys    = index_keys(primary);
	auto replace = recover;

	// add log record
	auto op = log_row(&tr->log, LOG_REPLACE, &log_if, primary, keys, row, NULL);
	log_persist(&tr->log, self->config->id);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	// todo
	/*
	// sync last serial column value during recover
	if (pos_serial && recover)
		part_sync_serial(self, pos_serial);
	*/

	// update primary index
	op->row_prev = index_set(primary, row);
	if (unlikely(op->row_prev && !replace))
		error("index '%.*s': unique key constraint violation",
		      str_size(&primary->config->name),
		      str_of(&primary->config->name));

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		op = log_row(&tr->log, LOG_REPLACE, &log_if_secondary, index, keys, row, NULL);
		op->row_prev = index_set(index, row);
		if (unlikely(op->row_prev && !replace))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}
}

hot void
part_update(Part* self, Tr* tr, Iterator* it, Row* row)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// add log record
	auto op = log_row(&tr->log, LOG_REPLACE, &log_if, primary, keys, row, NULL);
	log_persist(&tr->log, self->config->id);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	// update primary index
	op->row_prev = index_update(primary, row, it);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		op = log_row(&tr->log, LOG_REPLACE, &log_if_secondary, index, keys, row, NULL);

		// find and replace existing secondary row (keys are not updated)
		auto index_it = index_iterator(index);
		guard(iterator_close, index_it);
		iterator_open(index_it, row);
		op->row_prev = index_update(index, row, index_it);
	}
}

hot void
part_delete(Part* self, Tr* tr, Iterator* it)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);
	auto row = iterator_at(it);

	// add log record
	auto op = log_row(&tr->log, LOG_DELETE, &log_if, primary, keys, row, NULL);

	// update primary index
	op->row_prev = index_delete(primary, it);
	log_persist(&tr->log, self->config->id);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	// secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		op = log_row(&tr->log, LOG_DELETE, &log_if_secondary, index, keys, row, NULL);

		// delete by key
		op->row_prev = index_delete_by(index, row);
	}
}

hot void
part_delete_by(Part* self, Tr* tr, Row* row)
{
	// note: transaction memory limit is not ensured here
	// since this operation is used for recovery
	auto primary = part_primary(self);
	auto it = index_iterator(primary);
	guard(iterator_close, it);
	if (unlikely(! iterator_open(it, row)))
		error("delete by key does not match");
	part_delete(self, tr, it);
}

hot bool
part_upsert(Part* self, Tr* tr, Iterator* it, Row* row)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// add log record
	auto op = log_row(&tr->log, LOG_REPLACE, &log_if, primary, keys, row, NULL);
	log_persist(&tr->log, self->config->id);

	// ensure transaction log limit
	if (tr->limit)
		limit_ensure(tr->limit, row);

	// insert or get (iterator is openned in both cases)
	auto exists = index_upsert(primary, row, it);
	if (exists)
	{
		// note: original row is not freed
		log_truncate(&tr->log);
		return true;
	}

	// insert
	log_persist(&tr->log, self->config->id);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		op = log_row(&tr->log, LOG_REPLACE, &log_if_secondary, index, keys, row, NULL);
		op->row_prev = index_set(index, row);
		if (unlikely(op->row_prev))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}

	return false;
}

hot void
part_ingest_secondary(Part* self, Row* row)
{
	// update secondary indexes
	auto primary = part_primary(self);
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		index_ingest(index, row);
	}
}

hot void
part_ingest(Part* self, Row* row)
{
	auto primary = part_primary(self);

	/*
	// sync last serial column value during recover
	if (pos_serial)
		part_sync_serial(self, pos_serial);
	*/

	// update primary index
	index_ingest(primary, row);

	// secondary indexes
	part_ingest_secondary(self, row);
}
