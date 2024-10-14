
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

static Aggr*
aggr_count_create(AggrIf* iface, Value* init)
{
	unused(init);
	Aggr* self = am_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_count_free(Aggr* self)
{
	am_free(self);
}

static void
aggr_count_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	*count = 0;
}

static void
aggr_count_state_free(Aggr* self, uint8_t* state)
{
	unused(self);
	unused(state);
}

static int
aggr_count_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

static void
aggr_count_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	value_set_int(value, *count);
}

hot static void
aggr_count_write(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	int64_t* count = (int64_t*)state;
	(*count)++;
}

hot static void
aggr_count_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	*count += *(int64_t*)state_with;
}

AggrIf aggr_count =
{
	.create       = aggr_count_create,
	.free         = aggr_count_free,
	.state_create = aggr_count_state_create,
	.state_free   = aggr_count_state_free,
	.state_size   = aggr_count_state_size,
	.read         = aggr_count_read,
	.write        = aggr_count_write,
	.merge        = aggr_count_merge
};
