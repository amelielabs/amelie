
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
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>

static Aggr*
aggr_min_create(AggrIf* iface)
{
	Aggr* self = mn_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_min_free(Aggr* self)
{
	mn_free(self);
}

static void
aggr_min_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* min = (int64_t*)state;
	// min
	min[0] = INT64_MAX;
	// count
	min[1] = 0;
}

static int
aggr_min_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t) * 2;
}

hot static void
aggr_min_process(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	if (unlikely(value->type != VALUE_INT))
		error("min: aggr data integer expected");
	int64_t* min = (int64_t*)state;
	if (value->integer < *min)
		*min = value->integer;
	min[1]++;
}

hot static void
aggr_min_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* min = (int64_t*)state;
	int64_t* min_with = (int64_t*)state_with;
	if (min_with[1] == 0)
		return;
	if (*min_with < *min)
		*min = *min_with;
	min[1] += min_with[1];
}

static void
aggr_min_convert(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* min = (int64_t*)state;
	if (min[1] == 0)
	{
		value_set_int(value, 0);
		return;
	}
	value_set_int(value, *min);
}

AggrIf aggr_min =
{
	.create       = aggr_min_create,
	.free         = aggr_min_free,
	.state_create = aggr_min_state_create,
	.state_size   = aggr_min_state_size,
	.process      = aggr_min_process,
	.merge        = aggr_min_merge,
	.convert      = aggr_min_convert,
};
