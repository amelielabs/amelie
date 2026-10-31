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
		if (op->iface)
			op->iface->commit(log, op);
	}

	// reset log
	log_reset(log);
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
		if (op->iface)
			op->iface->abort(log, op);
	}

	// reset log
	log_reset(log);
}
