
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

static void
fn_abs(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_INT);
	value_set_int(self->result, llabs(arg->integer));
}

static void
fn_fabs(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, fabs(arg->dbl));
}

static void
fn_round(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	value_set_int(self->result, llround(arg->dbl));
}

static void
fn_sign(Call* self)
{
	auto arg = &self->argv[0];
	int sign = 0;
	call_validate(self, 1);
	if (arg->type == TYPE_INT)
	{
		if (arg->integer > 0)
			sign = 1;
		else
		if (arg->integer < 0)
			sign = -1;
	} else
	if (arg->type == TYPE_DOUBLE)
	{
		if (arg->dbl > 0.0)
			sign = 1;
		else
		if (arg->dbl < 0.0)
			sign = -1;
	} else {
		error("round(): int or double argument expected");
	}
	value_set_int(self->result, sign);
}

static void
fn_ceil(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, ceil(arg->dbl));
}

static void
fn_exp(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	errno = 0;
	double result = exp(arg->dbl);
	if (errno != 0)
		error("exp(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_floor(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, floor(arg->dbl));
}

static void
fn_mod(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_INT);
	call_validate_arg(self, 1, TYPE_INT);
	if (argv[1].integer == 0)
		error("mod(): zero division");
	int64_t result = argv[0].integer % argv[1].integer;
	value_set_int(self->result, result);
}

static void
fn_fmod(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	call_validate_arg(self, 1, TYPE_DOUBLE);
	if (argv[1].integer == 0)
		error("fmod(): zero division");
	double result = fmod(argv[0].dbl, argv[1].dbl);
	value_set_double(self->result, result);
}

static void
fn_pow(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_INT);
	call_validate_arg(self, 1, TYPE_INT);
	errno = 0;
	double result = pow(argv[0].integer, argv[1].integer);
	if (errno != 0)
		error("pow(): operation failed");
	value_set_int(self->result, (int64_t)result);
}

static void
fn_fpow(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	call_validate_arg(self, 1, TYPE_DOUBLE);
	errno = 0;
	double result = pow(argv[0].dbl, argv[1].dbl);
	if (errno != 0)
		error("fpow(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_trunc(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, trunc(arg->dbl));
}

static void
fn_pi(Call* self)
{
	call_validate(self, 0);
	value_set_double(self->result, 3.141592653589793);
}

static void
fn_sqrt(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("sqrt(): int or double arguments expected");
	errno = 0;
	double result = sqrt(value);
	if (errno != 0)
		error("sqrt(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_acos(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("acos(): int or double arguments expected");
	errno = 0;
	double result = acos(value);
	if (errno != 0)
		error("acos(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_acosh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("acosh(): int or double arguments expected");
	errno = 0;
	double result = acosh(value);
	if (errno != 0)
		error("acosh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_asin(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("asin(): int or double arguments expected");
	errno = 0;
	double result = asin(value);
	if (errno != 0)
		error("asin(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_asinh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("asinh(): int or double arguments expected");
	errno = 0;
	double result = asinh(value);
	if (errno != 0)
		error("asinh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("atan(): int or double arguments expected");
	errno = 0;
	double result = atan(value);
	if (errno != 0)
		error("atan(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_atanh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("atanh(): int or double arguments expected");
	errno = 0;
	double result = atanh(value);
	if (errno != 0)
		error("atanh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan2(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);

	double arg1;
	if (argv[0].type == TYPE_INT)
		arg1 = argv[0].integer;
	else
	if (argv[0].type == TYPE_DOUBLE)
		arg1 = argv[0].dbl;
	else
		error("atan2(): int or double arguments expected");

	double arg2;
	if (argv[1].type == TYPE_INT)
		arg2 = argv[1].integer;
	else
	if (argv[1].type == TYPE_DOUBLE)
		arg2 = argv[1].dbl;
	else
		error("atan2(): int or double arguments expected");

	errno = 0;
	double result = atan2(arg1, arg2);
	if (errno != 0)
		error("atan2(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_cos(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("cos(): int or double arguments expected");
	errno = 0;
	double result = cos(value);
	if (errno != 0)
		error("cos(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_cosh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("cosh(): int or double arguments expected");
	errno = 0;
	double result = cosh(value);
	if (errno != 0)
		error("cosh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_sin(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("sin(): int or double arguments expected");
	errno = 0;
	double result = sin(value);
	if (errno != 0)
		error("sin(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_sinh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("sinh(): int or double arguments expected");
	errno = 0;
	double result = sinh(value);
	if (errno != 0)
		error("sinh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_tan(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("tan(): int or double arguments expected");
	errno = 0;
	double result = tan(value);
	if (errno != 0)
		error("tan(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_tanh(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("tanh(): int or double arguments expected");
	errno = 0;
	double result = tanh(value);
	if (errno != 0)
		error("tanh(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_ln(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("ln(): int or double arguments expected");
	errno = 0;
	double result = log(value);
	if (errno != 0)
		error("ln(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_log(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("log(): int or double arguments expected");
	errno = 0;
	double result = log10(value);
	if (errno != 0)
		error("log(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_log2(Call* self)
{
	auto arg = &self->argv[0];
	call_validate(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		error("log2(): int or double arguments expected");
	errno = 0;
	double result = log2(value);
	if (errno != 0)
		error("log2(): operation failed");
	value_set_double(self->result, result);
}

static void
fn_greatest(Call* self)
{
	Value* max = NULL;
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i].type == TYPE_NULL)
			continue;
		if (max == NULL)
		{
			max = &self->argv[i];
			continue;
		}
		if (value_compare(&self->argv[i], max) == 1)
			max = &self->argv[i];
	}
	if (max)
		value_copy(self->result, max);
	else
		value_set_null(self->result);
}

static void
fn_least(Call* self)
{
	Value* max = NULL;
	for (int i = 0; i < self->argc; i++)
	{
		if (self->argv[i].type == TYPE_NULL)
			continue;
		if (max == NULL)
		{
			max = &self->argv[i];
			continue;
		}
		if (value_compare(&self->argv[i], max) == -1)
			max = &self->argv[i];
	}
	if (max)
		value_copy(self->result, max);
	else
		value_set_null(self->result);
}

FunctionDef fn_math_def[] =
{
	{ "public", "abs",      TYPE_INT,    fn_abs,      FN_NONE        },
	{ "public", "fabs",     TYPE_DOUBLE, fn_fabs,     FN_NONE        },
	{ "public", "round",    TYPE_INT,    fn_round,    FN_NONE        },
	{ "public", "sign",     TYPE_INT,    fn_sign,     FN_NONE        },
	{ "public", "ceil",     TYPE_DOUBLE, fn_ceil,     FN_NONE        },
	{ "public", "exp",      TYPE_DOUBLE, fn_exp,      FN_NONE        },
	{ "public", "floor",    TYPE_DOUBLE, fn_floor,    FN_NONE        },
	{ "public", "mod",      TYPE_INT,    fn_mod,      FN_NONE        },
	{ "public", "fmod",     TYPE_DOUBLE, fn_fmod,     FN_NONE        },
	{ "public", "pow",      TYPE_INT,    fn_pow,      FN_NONE        },
	{ "public", "fpow",     TYPE_DOUBLE, fn_fpow,     FN_NONE        },
	{ "public", "trunc",    TYPE_DOUBLE, fn_trunc,    FN_NONE        },
	{ "public", "pi",       TYPE_DOUBLE, fn_pi,       FN_NONE        },
	{ "public", "sqrt",     TYPE_DOUBLE, fn_sqrt,     FN_NONE        },
	{ "public", "acos",     TYPE_DOUBLE, fn_acos,     FN_NONE        },
	{ "public", "acosh",    TYPE_DOUBLE, fn_acosh,    FN_NONE        },
	{ "public", "asin",     TYPE_DOUBLE, fn_asin,     FN_NONE        },
	{ "public", "asinh",    TYPE_DOUBLE, fn_asinh,    FN_NONE        },
	{ "public", "atan",     TYPE_DOUBLE, fn_atan,     FN_NONE        },
	{ "public", "atanh",    TYPE_DOUBLE, fn_atanh,    FN_NONE        },
	{ "public", "atan2",    TYPE_DOUBLE, fn_atan2,    FN_NONE        },
	{ "public", "cos",      TYPE_DOUBLE, fn_cos,      FN_NONE        },
	{ "public", "cosh",     TYPE_DOUBLE, fn_cosh,     FN_NONE        },
	{ "public", "sin",      TYPE_DOUBLE, fn_sin,      FN_NONE        },
	{ "public", "sinh",     TYPE_DOUBLE, fn_sinh,     FN_NONE        },
	{ "public", "tan",      TYPE_DOUBLE, fn_tan,      FN_NONE        },
	{ "public", "tanh",     TYPE_DOUBLE, fn_tanh,     FN_NONE        },
	{ "public", "ln",       TYPE_DOUBLE, fn_ln,       FN_NONE        },
	{ "public", "log",      TYPE_DOUBLE, fn_log,      FN_NONE        },
	{ "public", "log10",    TYPE_DOUBLE, fn_log,      FN_NONE        },
	{ "public", "log2",     TYPE_DOUBLE, fn_log2,     FN_NONE        },
	{ "public", "greatest", TYPE_NULL,   fn_greatest, FN_TYPE_DERIVE },
	{ "public", "least",    TYPE_NULL,   fn_least,    FN_TYPE_DERIVE },
	{  NULL,     NULL,      TYPE_NULL,   NULL,        FN_NONE        }
};
