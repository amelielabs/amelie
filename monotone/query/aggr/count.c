
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
aggr_count_create(AggrIf* iface)
{
	Aggr* self = mn_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_count_free(Aggr* self)
{
	mn_free(self);
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

hot static void
aggr_count_process(Aggr* self, uint8_t* state, Value* value)
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

static void
aggr_count_convert(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	int64_t* count = (int64_t*)state;
	value_set_int(value, *count);
}

AggrIf aggr_count =
{
	.create       = aggr_count_create,
	.free         = aggr_count_free,
	.state_create = aggr_count_state_create,
	.state_size   = aggr_count_state_size,
	.process      = aggr_count_process,
	.merge        = aggr_count_merge,
	.convert      = aggr_count_convert,
};
