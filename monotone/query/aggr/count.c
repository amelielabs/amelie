
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
