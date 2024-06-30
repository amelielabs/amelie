
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
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

static Aggr*
aggr_lambda_create(AggrIf* iface)
{
	Aggr* self = so_malloc(sizeof(Aggr));
	self->iface = iface;
	list_init(&self->link);
	return self;
}

static void
aggr_lambda_free(Aggr* self)
{
	so_free(self);
}

static void
aggr_lambda_state_create(Aggr* self, uint8_t* state)
{
	unused(self);
	Value* ref = (Value*)state;
	value_init(ref);
	value_set_null(ref);
}

static void
aggr_lambda_state_free(Aggr* self, uint8_t* state)
{
	unused(self);
	Value* ref = (Value*)state;
	value_free(ref);
}

static int
aggr_lambda_state_size(Aggr* self)
{
	unused(self);
	return sizeof(Value);
}

static void
aggr_lambda_read(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	Value* ref = (Value*)state;
	value_copy(value, ref);
}

hot static void
aggr_lambda_write(Aggr* self, uint8_t* state, Value* value)
{
	unused(self);
	if (unlikely(value->type == VALUE_NULL))
		return;
	unused(self);
	Value* ref = (Value*)state;
	value_free(ref);
	value_copy(ref, value);
}

hot static void
aggr_lambda_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	unused(self);
	unused(state);
	unused(state_with);
	error("operation not supported");
}

AggrIf aggr_lambda =
{
	.create       = aggr_lambda_create,
	.free         = aggr_lambda_free,
	.state_create = aggr_lambda_state_create,
	.state_free   = aggr_lambda_state_free,
	.state_size   = aggr_lambda_state_size,
	.read         = aggr_lambda_read,
	.write        = aggr_lambda_write,
	.merge        = aggr_lambda_merge
};
