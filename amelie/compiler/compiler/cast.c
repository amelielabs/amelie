
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
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

typedef struct Cast Cast;

struct Cast
{
	int type;
	int op;
};

static Cast
cast_op[OP_MAX][TYPE_MAX][TYPE_MAX] =
{
	[OP_EQU] =
	{
		// bool
		[TYPE_BOOL][TYPE_BOOL]           = { TYPE_BOOL, CEQUII },
		[TYPE_BOOL][TYPE_INT]            = { TYPE_BOOL, CEQUII },

		// int
		[TYPE_INT][TYPE_BOOL]            = { TYPE_BOOL, CEQUII },
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CEQUII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CEQUIF },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CEQUII },
		[TYPE_INT][TYPE_DATE]            = { TYPE_BOOL, CEQUII },

		// double
		[TYPE_DOUBLE][TYPE_BOOL]         = { TYPE_BOOL, CEQUFI },
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CEQUFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CEQUFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_BOOL, CEQUII },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_BOOL, CEQUII },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CEQUII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CEQUII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CEQULL },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CEQUUU },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CEQUSS },

		// json
		[TYPE_JSON][TYPE_JSON]           = { TYPE_BOOL, CEQUJJ },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CEQUVV },
	},

	[OP_GTE] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CGTEII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CGTEIF },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CGTEII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CGTEFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CGTEFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_BOOL, CGTEII },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_BOOL, CGTEII },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CGTEII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CGTEII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CGTELL },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CGTEUU },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CGTESS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CGTEVV }
	},

	[OP_GT] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CGTII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CGTIF },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CGTII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CGTFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CGTFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_BOOL, CGTII },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_BOOL, CGTII },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CGTII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CGTII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CGTLL },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CGTUU },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CGTSS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CGTVV }
	},

	[OP_LTE] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CLTEII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CLTEIF },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CLTEII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CLTEFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CLTEFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_BOOL, CLTEII },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_BOOL, CLTEII },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CLTEII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CLTEII },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CLTEUU },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CLTELL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CLTESS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CLTEVV }
	},

	[OP_LT] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CLTII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CLTIF },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CLTII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CLTFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CLTFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_BOOL, CLTII },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_BOOL, CLTII },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CLTII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CLTII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CLTLL },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CLTUU },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CLTSS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CLTVV }
	},

	[OP_ADD] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CADDII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CADDIF },
		[TYPE_INT][TYPE_DATE]            = { TYPE_DATE,      CADDID },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CADDFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CADDFF },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_DATE,      CADDDI },
		[TYPE_DATE][TYPE_INTERVAL]       = { TYPE_TIMESTAMP, CADDDL },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INTERVAL]  = { TYPE_TIMESTAMP, CADDTL },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_INTERVAL,  CADDLL },
		[TYPE_INTERVAL][TYPE_TIMESTAMP]  = { TYPE_TIMESTAMP, CADDLT },
		[TYPE_INTERVAL][TYPE_DATE]       = { TYPE_TIMESTAMP, CADDLD },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CADDVV },
	},

	[OP_SUB] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CSUBII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CSUBIF },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CSUBFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CSUBFF },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INTERVAL]  = { TYPE_TIMESTAMP, CSUBTL },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_INTERVAL,  CSUBTT },

		// date
		[TYPE_DATE][TYPE_INT]            = { TYPE_DATE,      CSUBDI },
		[TYPE_DATE][TYPE_INTERVAL]       = { TYPE_TIMESTAMP, CSUBDL },
		[TYPE_DATE][TYPE_DATE]           = { TYPE_INT,       CSUBII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_INTERVAL,  CSUBLL },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CSUBVV },
	},

	[OP_MUL] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CMULII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CMULIF },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CMULFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CMULFF },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CMULVV },
	},

	[OP_DIV] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CDIVII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CDIVIF },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CDIVFI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CDIVFF },
	},

	[OP_MOD] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CMODII },
	},

	[OP_CAT] =
	{
		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_STRING,    CCATSS },
	},

	[OP_IDX] =
	{
		// json
		[TYPE_JSON][TYPE_STRING]         = { TYPE_JSON,      CIDXJS },
		[TYPE_JSON][TYPE_INT]            = { TYPE_JSON,      CIDXJI },

		// vector
		[TYPE_VECTOR][TYPE_INT]          = { TYPE_DOUBLE,    CIDXVI },
	},

	[OP_DOT] =
	{
		// json
		[TYPE_JSON][TYPE_STRING]         = { TYPE_JSON,      CDOTJS },
	},

	[OP_LIKE] =
	{
		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL,    CLIKESS },
	},

	[OP_BOR] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT, CBORII },
	},

	[OP_BAND] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT, CBANDII },
	},

	[OP_BXOR] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT, CBXORII },
	},

	[OP_BSHL] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT, CBSHLII },
	},

	[OP_BSHR] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT, CBSHRII },
	}
};

static const char*
cast_op_names[OP_MAX] =
{
	[OP_EQU]  = "=",
	[OP_GTE]  = ">=",
	[OP_GT]   = ">",
	[OP_LTE]  = "<=",
	[OP_LT]   = "<",
	[OP_ADD]  = "+",
	[OP_SUB]  = "-",
	[OP_MUL]  = "*",
	[OP_DIV]  = "/",
	[OP_MOD]  = "%",
	[OP_CAT]  = "||",
	[OP_IDX]  = "[]",
	[OP_DOT]  = ".",
	[OP_LIKE] = "LIKE",
	[OP_BOR]  = "|",
	[OP_BAND] = "&",
	[OP_BXOR] = "^",
	[OP_BSHL] = "<<",
	[OP_BSHR] = ">>"
};

hot int
cast_operator(Compiler* self, Ast* ast, int operator, int l, int r)
{
	assert(operator < OP_MAX);
	auto lt = rtype(self, l);
	auto rt = rtype(self, r);
	int  op;
	int  type;
	if (unlikely(lt == TYPE_NULL || rt == TYPE_NULL))
	{
		op   = CNULLOP;
		type = TYPE_NULL;
	} else
	{
		auto cast = &cast_op[operator][lt][rt];
		if (unlikely(cast->type == TYPE_NULL))
		{
			stmt_error(self->current, ast, "operation {s} {s} {s} is not supported",
			           type_of(lt), cast_op_names[operator],
			           type_of(rt));
		}
		op   = cast->op;
		type = cast->type;
	}
	int rc;
	rc = op3(self, op, rpin(self, type), l, r);
	runpin(self, l);
	runpin(self, r);
	return rc;
}
