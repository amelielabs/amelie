
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

static Aggr*
aggr_max_create(AggrIf* iface, Value* init)
{
	unused(init);
	Aggr* self = am_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_max_free(Aggr* self)
{
	am_free(self);
}

static void
aggr_max_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* max = (int64_t*)state;
	max[0] = 0;
}

static void
aggr_max_state_free(Aggr* self, uint8_t* state)
{
	unused(self);
	unused(state);
}

static int
aggr_max_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

static void
aggr_max_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* max = (int64_t*)state;
	value_set_int(value, *max);
}

hot static void
aggr_max_write(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	if (unlikely(value->type != VALUE_INT))
		error("max: aggr data integer expected");
	int64_t* max = (int64_t*)state;
	if (value->integer > *max)
		*max = value->integer;
}

hot static void
aggr_max_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* max = (int64_t*)state;
	int64_t* max_with = (int64_t*)state_with;
	if (*max_with > *max)
		*max = *max_with;
}

AggrIf aggr_max =
{
	.create       = aggr_max_create,
	.free         = aggr_max_free,
	.state_create = aggr_max_state_create,
	.state_free   = aggr_max_state_free,
	.state_size   = aggr_max_state_size,
	.read         = aggr_max_read,
	.write        = aggr_max_write,
	.merge        = aggr_max_merge
};
