
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
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

typedef struct
{
	Aggr  aggr;
	Value init;
} Lambda;

static inline Lambda*
lambda_of(Aggr* self)
{
	return (Lambda*)self;
}

static Aggr*
aggr_lambda_create(AggrIf* iface, Value* init)
{
	auto self = (Lambda*)am_malloc(sizeof(Lambda));
	self->aggr.iface = iface;
	list_init(&self->aggr.link);
	value_init(&self->init);
	value_copy(&self->init, init);
	return &self->aggr;
}

static void
aggr_lambda_free(Aggr* self)
{
	auto lambda = lambda_of(self);
	value_free(&lambda->init);
	am_free(self);
}

static void
aggr_lambda_state_create(Aggr* self, uint8_t* state)
{
	auto lambda = lambda_of(self);
	Value* ref = (Value*)state;
	value_init(ref);
	value_copy(ref, &lambda->init);
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
