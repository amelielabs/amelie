
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

static void
fn_abs(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_INT);
	value_set_int(self->result, llabs(arg->integer));
}

static void
fn_fabs(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, fabs(arg->dbl));
}

static void
fn_round(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_int(self->result, llround(arg->dbl));
}

static void
fn_sign(Call* self)
{
	auto arg = &self->argv[0];
	int sign = 0;
	call_expect(self, 1);
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
		call_unsupported(self, 0);
	}
	value_set_int(self->result, sign);
}

static void
fn_ceil(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, ceil(arg->dbl));
}

static void
fn_exp(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	errno = 0;
	double result = exp(arg->dbl);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_floor(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, floor(arg->dbl));
}

static void
fn_mod(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_expect_arg(self, 0, TYPE_INT);
	call_expect_arg(self, 1, TYPE_INT);
	if (argv[1].integer == 0)
		call_error_arg(self, 1, "zero division");
	int64_t result = argv[0].integer % argv[1].integer;
	value_set_int(self->result, result);
}

static void
fn_fmod(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	call_expect_arg(self, 1, TYPE_DOUBLE);
	if (argv[1].integer == 0)
		call_error_arg(self, 1, "zero division");
	double result = fmod(argv[0].dbl, argv[1].dbl);
	value_set_double(self->result, result);
}

static void
fn_pow(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_expect_arg(self, 0, TYPE_INT);
	call_expect_arg(self, 1, TYPE_INT);
	errno = 0;
	double result = pow(argv[0].integer, argv[1].integer);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_int(self->result, (int64_t)result);
}

static void
fn_fpow(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	call_expect_arg(self, 1, TYPE_DOUBLE);
	errno = 0;
	double result = pow(argv[0].dbl, argv[1].dbl);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_trunc(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	call_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, trunc(arg->dbl));
}

static void
fn_pi(Call* self)
{
	call_expect(self, 0);
	value_set_double(self->result, 3.141592653589793);
}

static void
fn_sqrt(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = sqrt(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_acos(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = acos(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_acosh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = acosh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_asin(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = asin(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_asinh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = asinh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = atan(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atanh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = atanh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan2(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);

	double arg1;
	if (argv[0].type == TYPE_INT)
		arg1 = argv[0].integer;
	else
	if (argv[0].type == TYPE_DOUBLE)
		arg1 = argv[0].dbl;
	else
		call_unsupported(self, 0);

	double arg2;
	if (argv[1].type == TYPE_INT)
		arg2 = argv[1].integer;
	else
	if (argv[1].type == TYPE_DOUBLE)
		arg2 = argv[1].dbl;
	else
		call_unsupported(self, 0);

	errno = 0;
	double result = atan2(arg1, arg2);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_cos(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = cos(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_cosh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = cosh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_sin(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = sin(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_sinh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = sinh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_tan(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = tan(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_tanh(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = tanh(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_ln(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = log(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_log(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = log10(value);
	if (errno != 0)
		call_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_log2(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		call_unsupported(self, 0);
	errno = 0;
	double result = log2(value);
	if (errno != 0)
		call_error(self, "operation failed");
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

void
fn_math_register(FunctionMgr* self)
{
	// public.abs()
	Function* func;
	func = function_allocate(TYPE_INT, "public", "abs", fn_abs);
	function_mgr_add(self, func);

	// public.fabs()
	func = function_allocate(TYPE_DOUBLE, "public", "fabs", fn_fabs);
	function_mgr_add(self, func);

	// public.round()
	func = function_allocate(TYPE_INT, "public", "round", fn_round);
	function_mgr_add(self, func);

	// public.sign()
	func = function_allocate(TYPE_INT, "public", "sign", fn_sign);
	function_mgr_add(self, func);

	// public.ceil()
	func = function_allocate(TYPE_DOUBLE, "public", "ceil", fn_ceil);
	function_mgr_add(self, func);

	// public.exp()
	func = function_allocate(TYPE_DOUBLE, "public", "exp", fn_exp);
	function_mgr_add(self, func);

	// public.floor()
	func = function_allocate(TYPE_DOUBLE, "public", "floor", fn_floor);
	function_mgr_add(self, func);

	// public.mod()
	func = function_allocate(TYPE_INT, "public", "mod", fn_mod);
	function_mgr_add(self, func);

	// public.fmod()
	func = function_allocate(TYPE_DOUBLE, "public", "fmod", fn_fmod);
	function_mgr_add(self, func);

	// public.pow()
	func = function_allocate(TYPE_INT, "public", "pow", fn_pow);
	function_mgr_add(self, func);

	// public.fpow()
	func = function_allocate(TYPE_DOUBLE, "public", "fpow", fn_fpow);
	function_mgr_add(self, func);

	// public.trunc()
	func = function_allocate(TYPE_DOUBLE, "public", "trunc", fn_trunc);
	function_mgr_add(self, func);

	// public.pi()
	func = function_allocate(TYPE_DOUBLE, "public", "pi", fn_pi);
	function_mgr_add(self, func);

	// public.sqrt()
	func = function_allocate(TYPE_DOUBLE, "public", "sqrt", fn_sqrt);
	function_mgr_add(self, func);

	// public.acos()
	func = function_allocate(TYPE_DOUBLE, "public", "acos", fn_acos);
	function_mgr_add(self, func);

	// public.acosh()
	func = function_allocate(TYPE_DOUBLE, "public", "acosh", fn_acosh);
	function_mgr_add(self, func);

	// public.asin()
	func = function_allocate(TYPE_DOUBLE, "public", "asin", fn_asin);
	function_mgr_add(self, func);

	// public.asinh()
	func = function_allocate(TYPE_DOUBLE, "public", "asinh", fn_asinh);
	function_mgr_add(self, func);

	// public.atan()
	func = function_allocate(TYPE_DOUBLE, "public", "atan", fn_atan);
	function_mgr_add(self, func);

	// public.atanh()
	func = function_allocate(TYPE_DOUBLE, "public", "atanh", fn_atanh);
	function_mgr_add(self, func);

	// public.atan2()
	func = function_allocate(TYPE_DOUBLE, "public", "atan2", fn_atan2);
	function_mgr_add(self, func);

	// public.cos()
	func = function_allocate(TYPE_DOUBLE, "public", "cos", fn_cos);
	function_mgr_add(self, func);

	// public.cosh()
	func = function_allocate(TYPE_DOUBLE, "public", "cosh", fn_cosh);
	function_mgr_add(self, func);

	// public.sin()
	func = function_allocate(TYPE_DOUBLE, "public", "sin", fn_sin);
	function_mgr_add(self, func);

	// public.sinh()
	func = function_allocate(TYPE_DOUBLE, "public", "sinh", fn_sinh);
	function_mgr_add(self, func);

	// public.tan()
	func = function_allocate(TYPE_DOUBLE, "public", "tan", fn_tan);
	function_mgr_add(self, func);

	// public.tanh()
	func = function_allocate(TYPE_DOUBLE, "public", "tanh", fn_tanh);
	function_mgr_add(self, func);

	// public.ln()
	func = function_allocate(TYPE_DOUBLE, "public", "ln", fn_ln);
	function_mgr_add(self, func);

	// public.log()
	func = function_allocate(TYPE_DOUBLE, "public", "log", fn_log);
	function_mgr_add(self, func);

	// public.log10()
	func = function_allocate(TYPE_DOUBLE, "public", "log10", fn_log);
	function_mgr_add(self, func);

	// public.log2()
	func = function_allocate(TYPE_DOUBLE, "public", "log2", fn_log2);
	function_mgr_add(self, func);

	// public.greatest()
	func = function_allocate(TYPE_NULL, "public", "greatest", fn_greatest);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);

	// public.least()
	func = function_allocate(TYPE_NULL, "public", "least", fn_least);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);
}
