
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

static Aggr*
aggr_count_create(AggrIf* iface)
{
	Aggr* self = so_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_count_free(Aggr* self)
{
	so_free(self);
}

static void
aggr_count_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	*count = 0;
}

static int
aggr_count_state_size(Aggr* self)
{
	unused(self);
	return sizeof(int64_t);
}

static void
aggr_count_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	value_set_int(value, *count);
}

hot static void
aggr_count_write(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	int64_t* count = (int64_t*)state;
	(*count)++;
}

hot static void
aggr_count_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	*count += *(int64_t*)state_with;
}

AggrIf aggr_count =
{
	.create       = aggr_count_create,
	.free         = aggr_count_free,
	.state_create = aggr_count_state_create,
	.state_size   = aggr_count_state_size,
	.read         = aggr_count_read,
	.write        = aggr_count_write,
	.merge        = aggr_count_merge
};
