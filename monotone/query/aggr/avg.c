
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
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_part.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>

static Aggr*
aggr_avg_create(AggrIf* iface)
{
	Aggr* self = mn_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_avg_free(Aggr* self)
{
	mn_free(self);
}

static void
aggr_avg_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* avg = (int64_t*)state;
	// sum
	avg[0] = 0;
	// count
	avg[1] = 0;
}

static int
aggr_avg_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t) * 2;
}

hot static void
aggr_avg_process(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	if (unlikely(value->type != VALUE_INT))
		error("avg: aggr data integer expected");
	int64_t* avg = (int64_t*)state;
	// sum
	avg[0] += value->integer;
	// count
	avg[1]++;
}

hot static void
aggr_avg_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* avg = (int64_t*)state;
	// sum
	avg[0] += ((int64_t*)state_with)[0];
	// count
	avg[1] += ((int64_t*)state_with)[1];
}

static void
aggr_avg_convert(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* avg = (int64_t*)state;
	if (avg[1] == 0)
		value_set_int(value, 0);
	else
		value_set_int(value, avg[0] / avg[1]);
}

AggrIf aggr_avg =
{
	.create       = aggr_avg_create,
	.free         = aggr_avg_free,
	.state_create = aggr_avg_state_create,
	.state_size   = aggr_avg_state_size,
	.process      = aggr_avg_process,
	.merge        = aggr_avg_merge,
	.convert      = aggr_avg_convert,
};
