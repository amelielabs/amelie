
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
aggr_sum_create(AggrIf* iface)
{
	Aggr* self = am_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_sum_free(Aggr* self)
{
	am_free(self);
}

static void
aggr_sum_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* sum = (int64_t*)state;
	*sum = 0;
}

static void
aggr_sum_state_free(Aggr* self, uint8_t* state)
{
	unused(self);
	unused(state);
}

static int
aggr_sum_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

static void
aggr_sum_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* sum = (int64_t*)state;
	value_set_int(value, *sum);
}

hot static void
aggr_sum_write(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	if (unlikely(value->type != VALUE_INT))
		error("sum: aggr data integer expected");
	int64_t* sum = (int64_t*)state;
	(*sum) += value->integer;
}

hot static void
aggr_sum_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* sum = (int64_t*)state;
	*sum += *(int64_t*)state_with;
}

AggrIf aggr_sum =
{
	.create       = aggr_sum_create,
	.free         = aggr_sum_free,
	.state_create = aggr_sum_state_create,
	.state_free   = aggr_sum_state_free,
	.state_size   = aggr_sum_state_size,
	.read         = aggr_sum_read,
	.write        = aggr_sum_write,
	.merge        = aggr_sum_merge
};
