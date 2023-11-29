
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>

static Aggr*
aggr_sum_create(AggrIf* iface)
{
	Aggr* self = mn_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_sum_free(Aggr* self)
{
	mn_free(self);
}

static void
aggr_sum_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* sum = (int64_t*)state;
	*sum = 0;
}

static int
aggr_sum_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

hot static void
aggr_sum_process(Aggr* self, uint8_t* state, Value* value)
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

static void
aggr_sum_convert(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* sum = (int64_t*)state;
	value_set_int(value, *sum);
}

AggrIf aggr_sum =
{
	.create       = aggr_sum_create,
	.free         = aggr_sum_free,
	.state_create = aggr_sum_state_create,
	.state_size   = aggr_sum_state_size,
	.process      = aggr_sum_process,
	.merge        = aggr_sum_merge,
	.convert      = aggr_sum_convert,
};
