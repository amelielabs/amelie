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

typedef struct OpExpr OpExpr;

struct OpExpr
{
	int type;
	int op;
};

extern OpExpr op_equ[VALUE_MAX][VALUE_MAX];
extern OpExpr op_bor[VALUE_MAX][VALUE_MAX];
extern OpExpr op_band[VALUE_MAX][VALUE_MAX];
extern OpExpr op_bxor[VALUE_MAX][VALUE_MAX];
extern OpExpr op_bshl[VALUE_MAX][VALUE_MAX];
extern OpExpr op_bshr[VALUE_MAX][VALUE_MAX];
extern OpExpr op_equ[VALUE_MAX][VALUE_MAX];
extern OpExpr op_gte[VALUE_MAX][VALUE_MAX];
extern OpExpr op_gt[VALUE_MAX][VALUE_MAX];
extern OpExpr op_lte[VALUE_MAX][VALUE_MAX];
extern OpExpr op_lt[VALUE_MAX][VALUE_MAX];
extern OpExpr op_add[VALUE_MAX][VALUE_MAX];
extern OpExpr op_sub[VALUE_MAX][VALUE_MAX];
extern OpExpr op_mul[VALUE_MAX][VALUE_MAX];
extern OpExpr op_div[VALUE_MAX][VALUE_MAX];
extern OpExpr op_cat[VALUE_MAX][VALUE_MAX];

extern OpExpr op_cursor[CURSOR_MAX][TYPE_MAX];
