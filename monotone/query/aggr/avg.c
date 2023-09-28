
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
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
aggr_avg_state_create(Aggr* self, uint8_t* state_data)
{
	unused(self);
	int64_t* state = (int64_t*)state_data;
	// sum
	state[0] = 0;
	// count
	state[1] = 0;
}

static int
aggr_avg_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t) * 2;
}

hot static void
aggr_avg_process(Aggr* self, uint8_t* state_data, Value* value)
{
	unused(self);
	if (unlikely(value->type != VALUE_INT))
		error("avg: aggr data integer expected");
	int64_t* state = (int64_t*)state_data;
	// sum
	state[0] += value->integer;
	// count
	state[1]++;
}

static void
aggr_avg_convert(Aggr* self, uint8_t* state_data, Value* value)
{
	unused(self);
	int64_t* state = (int64_t*)state_data;
	assert(state[1] > 0);
	value_set_int(value, state[0] / state[1]);
}

AggrIf aggr_avg =
{
	.create       = aggr_avg_create,
	.free         = aggr_avg_free,
	.state_create = aggr_avg_state_create,
	.state_size   = aggr_avg_state_size,
	.process      = aggr_avg_process,
	.convert      = aggr_avg_convert,
};
