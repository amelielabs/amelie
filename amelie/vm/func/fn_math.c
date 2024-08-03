
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
	call_validate_arg(self, 0, VALUE_REAL);
	value_set_int(self->result, llround(arg->real));
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

hot static void
fn_ceil(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_REAL);
	value_set_real(self->result, ceil(arg->real));
}

hot static void
fn_exp(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_REAL);
	errno = 0;
	double result = exp(arg->real);
	if (errno != 0)
		error("exp(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_floor(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_REAL);
	value_set_real(self->result, floor(arg->real));
}

hot static void
fn_mod(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	if (argv[0]->type == VALUE_INT)
	{
		if (argv[1]->type != VALUE_INT)
			goto error;
		if (argv[1]->integer == 0)
			error("mod(): zero division");
		int64_t result = argv[0]->integer % argv[1]->integer;
		value_set_int(self->result, result);
	} else
	if (argv[0]->type == VALUE_REAL)
	{
		if (argv[1]->type != VALUE_REAL)
			goto error;
		if (argv[1]->real == 0)
			error("mod(): zero division");
		double result = fmod(argv[0]->real, argv[1]->real);
		value_set_real(self->result, result);
	} else {
		goto error;
	}
	return;
error:
	error("mod(): int or real arguments expected");
}

hot static void
fn_power(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	if (argv[0]->type == VALUE_INT)
	{
		if (argv[1]->type != VALUE_INT)
			goto error;
		errno = 0;
		double result = pow(argv[0]->integer, argv[1]->integer);
		if (errno != 0)
			error("power(): operation failed");
		value_set_int(self->result, (int64_t)result);
	} else
	if (argv[0]->type == VALUE_REAL)
	{
		if (argv[1]->type != VALUE_REAL)
			goto error;
		errno = 0;
		double result = pow(argv[0]->real, argv[1]->real);
		if (errno != 0)
			error("power(): operation failed");
		value_set_real(self->result, result);
	} else {
		goto error;
	}
	return;
error:
	error("power(): int or real arguments expected");
}

hot static void
fn_trunc(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_REAL);
	value_set_real(self->result, trunc(arg->real));
}

hot static void
fn_pi(Call* self)
{
	call_validate(self);
	value_set_real(self->result, 3.141592653589793);
}

hot static void
fn_sqrt(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("sqrt(): int or real arguments expected");
	errno = 0;
	double result = sqrt(value);
	if (errno != 0)
		error("sqrt(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_acos(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("acos(): int or real arguments expected");
	errno = 0;
	double result = acos(value);
	if (errno != 0)
		error("acos(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_acosh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("acosh(): int or real arguments expected");
	errno = 0;
	double result = acosh(value);
	if (errno != 0)
		error("acosh(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_asin(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("asin(): int or real arguments expected");
	errno = 0;
	double result = asin(value);
	if (errno != 0)
		error("asin(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_asinh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("asinh(): int or real arguments expected");
	errno = 0;
	double result = asinh(value);
	if (errno != 0)
		error("asinh(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_atan(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("atan(): int or real arguments expected");
	errno = 0;
	double result = atan(value);
	if (errno != 0)
		error("atan(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_atanh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("atanh(): int or real arguments expected");
	errno = 0;
	double result = atanh(value);
	if (errno != 0)
		error("atanh(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_atan2(Call* self)
{
	auto argv = self->argv;
	call_validate(self);

	double arg1;
	if (argv[0]->type == VALUE_INT)
		arg1 = argv[0]->integer;
	else
	if (argv[0]->type == VALUE_REAL)
		arg1 = argv[0]->real;
	else
		error("atan2(): int or real arguments expected");

	double arg2;
	if (argv[1]->type == VALUE_INT)
		arg2 = argv[1]->integer;
	else
	if (argv[1]->type == VALUE_REAL)
		arg2 = argv[1]->real;
	else
		error("atan2(): int or real arguments expected");

	errno = 0;
	double result = atan2(arg1, arg2);
	if (errno != 0)
		error("atan2(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_cos(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("cos(): int or real arguments expected");
	errno = 0;
	double result = cos(value);
	if (errno != 0)
		error("cos(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_cosh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("cosh(): int or real arguments expected");
	errno = 0;
	double result = cosh(value);
	if (errno != 0)
		error("cosh(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_sin(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("sin(): int or real arguments expected");
	errno = 0;
	double result = sin(value);
	if (errno != 0)
		error("sin(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_sinh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("sinh(): int or real arguments expected");
	errno = 0;
	double result = sinh(value);
	if (errno != 0)
		error("sinh(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_tan(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("tan(): int or real arguments expected");
	errno = 0;
	double result = tan(value);
	if (errno != 0)
		error("tan(): operation failed");
	value_set_real(self->result, result);
}

hot static void
fn_tanh(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	double value;
	if (arg->type == VALUE_INT)
		value = arg->integer;
	else
	if (arg->type == VALUE_REAL)
		value = arg->real;
	else
		error("tanh(): int or real arguments expected");
	errno = 0;
	double result = tanh(value);
	if (errno != 0)
		error("tanh(): operation failed");
	value_set_real(self->result, result);
}

FunctionDef fn_math_def[] =
{
	{ "public", "greatest", fn_greatest, 0 },
	{ "public", "least",    fn_least,    0 },
	{ "public", "abs",      fn_abs,      1 },
	{ "public", "round",    fn_round,    1 },
	{ "public", "sign",     fn_sign,     1 },
	{ "public", "ceil",     fn_ceil,     1 },
	{ "public", "exp",      fn_exp,      1 },
	{ "public", "floor",    fn_floor,    1 },
	{ "public", "mod",      fn_mod,      2 },
	{ "public", "power",    fn_power,    2 },
	{ "public", "trunc",    fn_trunc,    1 },
	{ "public", "pi",       fn_pi,       0 },
	{ "public", "sqrt",     fn_sqrt,     1 },

	{ "public", "acos",     fn_acos,     1 },
	{ "public", "acosh",    fn_acosh,    1 },
	{ "public", "asin",     fn_asin,     1 },
	{ "public", "asinh",    fn_asinh,    1 },
	{ "public", "atan",     fn_atan,     1 },
	{ "public", "atanh",    fn_atanh,    1 },
	{ "public", "atan2",    fn_atan2,    2 },
	{ "public", "cos",      fn_cos,      1 },
	{ "public", "cosh",     fn_cosh,     1 },
	{ "public", "sin",      fn_sin,      1 },
	{ "public", "sinh",     fn_sinh,     1 },
	{ "public", "tan",      fn_tan,      1 },
	{ "public", "tanh",     fn_tanh,     1 },

	{  NULL,     NULL,      NULL,        0 }
};
