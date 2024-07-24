
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
aggr_min_create(AggrIf* iface)
{
	Aggr* self = am_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_min_free(Aggr* self)
{
	am_free(self);
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

static void
aggr_min_state_free(Aggr* self, uint8_t* state)
{
	unused(self);
	unused(state);
}

static int
aggr_min_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t) * 2;
}

static void
aggr_min_read(Aggr* self, uint8_t* state, Value* value)
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

hot static void
aggr_min_write(Aggr* self, uint8_t* state, Value* value)
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

AggrIf aggr_min =
{
	.create       = aggr_min_create,
	.free         = aggr_min_free,
	.state_create = aggr_min_state_create,
	.state_free   = aggr_min_state_free,
	.state_size   = aggr_min_state_size,
	.read         = aggr_min_read,
	.write        = aggr_min_write,
	.merge        = aggr_min_merge
};
