#pragma once

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

enum
{
	OP_EQU,
	OP_GTE,
	OP_GT,
	OP_LTE,
	OP_LT,
	OP_ADD,
	OP_SUB,
	OP_MUL,
	OP_DIV,
	OP_MOD,
	OP_CAT,
	OP_IDX,
	OP_DOT,
	OP_LIKE,
	OP_BOR,
	OP_BAND,
	OP_BXOR,
	OP_BSHL,
	OP_BSHR,
	OP_MAX
};

int cast_operator(Compiler*, Ast*, int, int, int);
