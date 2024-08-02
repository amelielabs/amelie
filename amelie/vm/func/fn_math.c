
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
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_greatest(Call* self)
{
	Value* max = NULL;
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i]->type == VALUE_NULL)
			continue;
		if (max == NULL)
		{
			max = self->argv[i];
			continue;
		}
		if (max->type != self->argv[i]->type)
			error("greatest(): types mismatch");
		if (value_compare(self->argv[i], max) == 1)
			max = self->argv[i];
	}
	if (max)
		value_copy(self->result, max);
	else
		value_set_null(self->result);
}

hot static void
fn_least(Call* self)
{
	Value* max = NULL;
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i]->type == VALUE_NULL)
			continue;
		if (max == NULL)
		{
			max = self->argv[i];
			continue;
		}
		if (max->type != self->argv[i]->type)
			error("least(): types mismatch");
		if (value_compare(self->argv[i], max) == -1)
			max = self->argv[i];
	}
	if (max)
		value_copy(self->result, max);
	else
		value_set_null(self->result);
}

hot static void
fn_abs(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	if (arg->type == VALUE_INT)
		value_set_int(self->result, llabs(arg->integer));
	else
	if (arg->type == VALUE_REAL)
		value_set_real(self->result, fabs(arg->real));
	else
		error("abs(): int or real argument expected");
}

hot static void
fn_round(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	if (arg->type == VALUE_INT)
		value_set_int(self->result, arg->integer);
	else
	if (arg->type == VALUE_REAL)
		value_set_int(self->result, llround(arg->real));
	else
		error("round(): int or real argument expected");
}

hot static void
fn_sign(Call* self)
{
	auto arg = self->argv[0];
	int sign = 0;
	call_validate(self);
	if (arg->type == VALUE_INT)
	{
		if (arg->integer > 0)
			sign = 1;
		else
		if (arg->integer < 0)
			sign = -1;
	} else
	if (arg->type == VALUE_REAL)
	{
		if (arg->real > 0.0)
			sign = 1;
		else
		if (arg->real < 0.0)
			sign = -1;
	} else {
		error("round(): int or real argument expected");
	}
	value_set_int(self->result, sign);
}

FunctionDef fn_math_def[] =
{
	{ "public", "greatest", fn_greatest, 0 },
	{ "public", "least",    fn_least,    0 },
	{ "public", "abs",      fn_abs,      1 },
	{ "public", "round",    fn_round,    1 },
	{ "public", "sign",     fn_sign,     1 },
	{  NULL,     NULL,      NULL,        0 }
};
