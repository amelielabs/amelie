#pragma once

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

hot static inline void
tr_commit(Tr* self)
{
	if (! tr_active(self))
		return;

	auto log = &self->log;
	for (int pos = 0; pos < log->count; pos++)
	{
		auto op = log_of(log, pos);
		op->iface->commit(log, op);
	}

	// reset log
	log_reset(&self->log);
}

static inline void
tr_abort(Tr* self)
{
	if (! tr_active(self))
		return;

	auto log = &self->log;
	for (int pos = self->log.count - 1; pos >= 0; pos--)
	{
		auto op = log_of(log, pos);
		op->iface->abort(log, op);
	}

	// reset log
	log_reset(&self->log);
}

hot static inline void
tr_commit_list(TrList* self, TrCache* cache, uint64_t id)
{
	// commit all transactions <= id
	list_foreach_safe(&self->list)
	{
		auto tr = list_at(Tr, link);
		if (tr->id <= id)
		{
			list_unlink(&tr->link);
			self->list_count--;
			tr_commit(tr);
			tr_cache_push(cache, tr);
		}
	}
}

static inline void
tr_abort_list(TrList* self, TrCache* cache, uint64_t id)
{
	// abort all transactions <= id
	list_foreach_reverse_safe(&self->list)
	{
		auto tr = list_at(Tr, link);
		if (tr->id <= id)
		{
			list_unlink(&tr->link);
			self->list_count--;
			tr_abort(tr);
			tr_cache_push(cache, tr);
		}
	}
}
