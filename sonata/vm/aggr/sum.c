
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
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

static Aggr*
aggr_sum_create(AggrIf* iface)
{
	Aggr* self = so_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_sum_free(Aggr* self)
{
	so_free(self);
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
	.state_size   = aggr_sum_state_size,
	.read         = aggr_sum_read,
	.write        = aggr_sum_write,
	.merge        = aggr_sum_merge
};
