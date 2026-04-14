
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
ack_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
ack_if_abort(Log* self, LogOp* op)
{
	// restore subscription
	uint8_t* pos = log_data_of(self, op);
	int64_t prev;
	json_read_integer(&pos, &prev);

	auto sub = sub_of(op->rel);
	cdc_slot_set(&sub->slot, prev);
}

static LogIf ack_if =
{
	.commit = ack_if_commit,
	.abort  = ack_if_abort
};

bool
acknowledge(Sub* self, Tr* tr, uint8_t* op)
{
	// only owner or superuser
	check_ownership_user(tr, &self->rel);

	int64_t to;
	acknowledge_op_read(op, &to);

	// do nothing, if value is the same
	int64_t current = atomic_u64_of(&self->slot.lsn);
	if (to == current)
		return false;

	// ensure value is valid
	if (unlikely(to < current || to > (int64_t)state_lsn()))
		error("subscription '%.*s': ack lsn is out of range",
		      str_size(&self->config->name), str_of(&self->config->name));

	// update subscription slot
	log_cmd(&tr->log, CMD_ACK, &ack_if, NULL, &self->rel);
	log_persist_cmd(&tr->log, &self->config->id, op);

	// save previous value
	encode_integer(&tr->log.data, current);

	// update slot
	//
	// This assumes global catalog shared lock is taken to
	// avoid any concurrent wal gc/checkpoint till this transaction
	// completes
	//
	cdc_slot_set(&self->slot, to);
	return true;
}
