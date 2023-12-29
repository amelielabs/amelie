
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>

static Aggr*
aggr_max_create(AggrIf* iface)
{
	Aggr* self = mn_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_max_free(Aggr* self)
{
	mn_free(self);
}

static void
aggr_max_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* max = (int64_t*)state;
	max[0] = 0;
}

static int
aggr_max_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

hot static void
aggr_max_process(Aggr* self, uint8_t* state, Value* value)
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

static void
aggr_max_convert(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* max = (int64_t*)state;
	value_set_int(value, *max);
}

AggrIf aggr_max =
{
	.create       = aggr_max_create,
	.free         = aggr_max_free,
	.state_create = aggr_max_state_create,
	.state_size   = aggr_max_state_size,
	.process      = aggr_max_process,
	.merge        = aggr_max_merge,
	.convert      = aggr_max_convert,
};
