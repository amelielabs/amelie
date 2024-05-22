
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

static Aggr*
aggr_avg_create(AggrIf* iface)
{
	Aggr* self = so_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_avg_free(Aggr* self)
{
	so_free(self);
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

static void
aggr_avg_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* avg = (int64_t*)state;
	if (avg[1] == 0)
		value_set_int(value, 0);
	else
		value_set_int(value, avg[0] / avg[1]);
}

hot static void
aggr_avg_write(Aggr* self, uint8_t* state, Value* value)
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

AggrIf aggr_avg =
{
	.create       = aggr_avg_create,
	.free         = aggr_avg_free,
	.state_create = aggr_avg_state_create,
	.state_size   = aggr_avg_state_size,
	.read         = aggr_avg_read,
	.write        = aggr_avg_write,
	.merge        = aggr_avg_merge
};
