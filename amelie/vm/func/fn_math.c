
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>

static void
fn_abs(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	fn_expect_arg(self, 0, TYPE_INT);
	value_set_int(self->result, llabs(arg->integer));
}

static void
fn_fabs(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, fabs(arg->dbl));
}

static void
fn_round(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_int(self->result, llround(arg->dbl));
}

static void
fn_sign(Fn* self)
{
	auto arg = &self->argv[0];
	int sign = 0;
	fn_expect(self, 1);
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
		fn_unsupported(self, 0);
	}
	value_set_int(self->result, sign);
}

static void
fn_ceil(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, ceil(arg->dbl));
}

static void
fn_exp(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	errno = 0;
	double result = exp(arg->dbl);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_floor(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, floor(arg->dbl));
}

static void
fn_mod(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_INT);
	fn_expect_arg(self, 1, TYPE_INT);
	if (argv[1].integer == 0)
		fn_error_arg(self, 1, "zero division");
	int64_t result = argv[0].integer % argv[1].integer;
	value_set_int(self->result, result);
}

static void
fn_fmod(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	fn_expect_arg(self, 1, TYPE_DOUBLE);
	if (argv[1].integer == 0)
		fn_error_arg(self, 1, "zero division");
	double result = fmod(argv[0].dbl, argv[1].dbl);
	value_set_double(self->result, result);
}

static void
fn_pow(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_INT);
	fn_expect_arg(self, 1, TYPE_INT);
	errno = 0;
	double result = pow(argv[0].integer, argv[1].integer);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_int(self->result, (int64_t)result);
}

static void
fn_fpow(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	fn_expect_arg(self, 1, TYPE_DOUBLE);
	errno = 0;
	double result = pow(argv[0].dbl, argv[1].dbl);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_trunc(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_DOUBLE);
	value_set_double(self->result, trunc(arg->dbl));
}

static void
fn_pi(Fn* self)
{
	fn_expect(self, 0);
	value_set_double(self->result, 3.141592653589793);
}

static void
fn_sqrt(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = sqrt(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_acos(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = acos(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_acosh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = acosh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_asin(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = asin(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_asinh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = asinh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = atan(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atanh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = atanh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_atan2(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 2);

	double arg1;
	if (argv[0].type == TYPE_INT)
		arg1 = argv[0].integer;
	else
	if (argv[0].type == TYPE_DOUBLE)
		arg1 = argv[0].dbl;
	else
		fn_unsupported(self, 0);

	double arg2;
	if (argv[1].type == TYPE_INT)
		arg2 = argv[1].integer;
	else
	if (argv[1].type == TYPE_DOUBLE)
		arg2 = argv[1].dbl;
	else
		fn_unsupported(self, 0);

	errno = 0;
	double result = atan2(arg1, arg2);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_cos(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = cos(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_cosh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = cosh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_sin(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = sin(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_sinh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = sinh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_tan(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = tan(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_tanh(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = tanh(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_ln(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = log(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_log(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = log10(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_log2(Fn* self)
{
	auto arg = &self->argv[0];
	fn_expect(self, 1);
	double value;
	if (arg->type == TYPE_INT)
		value = arg->integer;
	else
	if (arg->type == TYPE_DOUBLE)
		value = arg->dbl;
	else
		fn_unsupported(self, 0);
	errno = 0;
	double result = log2(value);
	if (errno != 0)
		fn_error(self, "operation failed");
	value_set_double(self->result, result);
}

static void
fn_greatest(Fn* self)
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
fn_least(Fn* self)
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
	// abs()
	Function* func;
	func = function_allocate(TYPE_INT, "abs", fn_abs);
	function_mgr_add(self, func);

	// fabs()
	func = function_allocate(TYPE_DOUBLE, "fabs", fn_fabs);
	function_mgr_add(self, func);

	// round()
	func = function_allocate(TYPE_INT, "round", fn_round);
	function_mgr_add(self, func);

	// sign()
	func = function_allocate(TYPE_INT, "sign", fn_sign);
	function_mgr_add(self, func);

	// ceil()
	func = function_allocate(TYPE_DOUBLE, "ceil", fn_ceil);
	function_mgr_add(self, func);

	// exp()
	func = function_allocate(TYPE_DOUBLE, "exp", fn_exp);
	function_mgr_add(self, func);

	// floor()
	func = function_allocate(TYPE_DOUBLE, "floor", fn_floor);
	function_mgr_add(self, func);

	// mod()
	func = function_allocate(TYPE_INT, "mod", fn_mod);
	function_mgr_add(self, func);

	// fmod()
	func = function_allocate(TYPE_DOUBLE, "fmod", fn_fmod);
	function_mgr_add(self, func);

	// pow()
	func = function_allocate(TYPE_INT, "pow", fn_pow);
	function_mgr_add(self, func);

	// fpow()
	func = function_allocate(TYPE_DOUBLE, "fpow", fn_fpow);
	function_mgr_add(self, func);

	// trunc()
	func = function_allocate(TYPE_DOUBLE, "trunc", fn_trunc);
	function_mgr_add(self, func);

	// pi()
	func = function_allocate(TYPE_DOUBLE, "pi", fn_pi);
	function_mgr_add(self, func);

	// sqrt()
	func = function_allocate(TYPE_DOUBLE, "sqrt", fn_sqrt);
	function_mgr_add(self, func);

	// acos()
	func = function_allocate(TYPE_DOUBLE, "acos", fn_acos);
	function_mgr_add(self, func);

	// acosh()
	func = function_allocate(TYPE_DOUBLE, "acosh", fn_acosh);
	function_mgr_add(self, func);

	// asin()
	func = function_allocate(TYPE_DOUBLE, "asin", fn_asin);
	function_mgr_add(self, func);

	// asinh()
	func = function_allocate(TYPE_DOUBLE, "asinh", fn_asinh);
	function_mgr_add(self, func);

	// atan()
	func = function_allocate(TYPE_DOUBLE, "atan", fn_atan);
	function_mgr_add(self, func);

	// atanh()
	func = function_allocate(TYPE_DOUBLE, "atanh", fn_atanh);
	function_mgr_add(self, func);

	// atan2()
	func = function_allocate(TYPE_DOUBLE, "atan2", fn_atan2);
	function_mgr_add(self, func);

	// cos()
	func = function_allocate(TYPE_DOUBLE, "cos", fn_cos);
	function_mgr_add(self, func);

	// cosh()
	func = function_allocate(TYPE_DOUBLE, "cosh", fn_cosh);
	function_mgr_add(self, func);

	// sin()
	func = function_allocate(TYPE_DOUBLE, "sin", fn_sin);
	function_mgr_add(self, func);

	// sinh()
	func = function_allocate(TYPE_DOUBLE, "sinh", fn_sinh);
	function_mgr_add(self, func);

	// tan()
	func = function_allocate(TYPE_DOUBLE, "tan", fn_tan);
	function_mgr_add(self, func);

	// tanh()
	func = function_allocate(TYPE_DOUBLE, "tanh", fn_tanh);
	function_mgr_add(self, func);

	// ln()
	func = function_allocate(TYPE_DOUBLE, "ln", fn_ln);
	function_mgr_add(self, func);

	// log()
	func = function_allocate(TYPE_DOUBLE, "log", fn_log);
	function_mgr_add(self, func);

	// log10()
	func = function_allocate(TYPE_DOUBLE, "log10", fn_log);
	function_mgr_add(self, func);

	// log2()
	func = function_allocate(TYPE_DOUBLE, "log2", fn_log2);
	function_mgr_add(self, func);

	// greatest()
	func = function_allocate(TYPE_NULL, "greatest", fn_greatest);
	function_unset(func, FN_CONST);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);

	// least()
	func = function_allocate(TYPE_NULL, "least", fn_least);
	function_unset(func, FN_CONST);
	function_set(func, FN_DERIVE);
	function_mgr_add(self, func);
}
