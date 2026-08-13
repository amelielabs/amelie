
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
#include <amelie_catalog.h>

static void
ack_if_commit(Log* self, LogOp* op)
{
	unused(self);
	// cdc gc
	auto sub = sub_of(op->rel);
	cdc_gc(sub->catalog->cdc);
}

static void
ack_if_abort(Log* self, LogOp* op)
{
	// restore subscription
	uint8_t* pos = log_data_of(self, op);

	int64_t pos_lsn;
	unpack_int(&pos, &pos_lsn);

	auto sub = sub_of(op->rel);
	sub_config_set_pos(sub->config, pos_lsn);
	cdc_slot_set(&sub->slot, pos_lsn);
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

	int64_t lsn;
	acknowledge_op_read(op, &lsn);

	// do nothing, if value is the same
	int64_t current_lsn = atomic_u64_of(&self->slot.lsn);
	if (lsn == current_lsn)
		return false;

	// ensure value is valid
	if (lsn < current_lsn || lsn > (int64_t)state_lsn())
		error("subscription '{str}': ack position is out of range",
		      &self->config->name);

	// update subscription slot
	log_cmd(&tr->log, LOG_ACK, &ack_if, NULL, &self->rel);

	// save previous value
	encode_int(&tr->log.data, current_lsn);

	// update slot
	//
	// This assumes global catalog shared lock is taken to
	// avoid any concurrent wal gc/checkpoint till this transaction
	// completes
	//
	sub_config_set_pos(self->config, lsn);
	cdc_slot_set(&self->slot, lsn);
	return true;
}
