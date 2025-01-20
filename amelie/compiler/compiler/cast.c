
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
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
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CEQUID },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CEQUII },

		// double
		[TYPE_DOUBLE][TYPE_BOOL]         = { TYPE_BOOL, CEQUDI },
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CEQUDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CEQUDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CEQUII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CEQUII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CEQULL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CEQUSS },

		// json
		[TYPE_JSON][TYPE_JSON]           = { TYPE_BOOL, CEQUJJ },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CEQUVV },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CEQUUU },
	},

	[OP_GTE] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CGTEII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CGTEID },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CGTEII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CGTEDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CGTEDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CGTEII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CGTEII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CGTELL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CGTESS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CGTEVV },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CGTEUU }
	},

	[OP_GT] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CGTII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CGTID },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CGTII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CGTDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CGTDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CGTII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CGTII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CGTLL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CGTSS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CGTVV },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CGTUU }
	},

	[OP_LTE] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CLTEII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CLTEID },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CLTEII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CLTEDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CLTEDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CLTEII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CLTEII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CLTELL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CLTESS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CLTEVV },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CLTEUU }
	},

	[OP_LT] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_BOOL, CLTII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_BOOL, CLTID },
		[TYPE_INT][TYPE_TIMESTAMP]       = { TYPE_BOOL, CLTII },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_BOOL, CLTDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_BOOL, CLTDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INT]       = { TYPE_BOOL, CLTII },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_BOOL, CLTII },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_BOOL, CLTLL },

		// string
		[TYPE_STRING][TYPE_STRING]       = { TYPE_BOOL, CLTSS },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_BOOL, CLTVV },

		// uuid
		[TYPE_UUID][TYPE_UUID]           = { TYPE_BOOL, CLTUU }
	},

	[OP_ADD] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CADDII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CADDID },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CADDDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CADDDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INTERVAL]  = { TYPE_TIMESTAMP, CADDTL },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_INTERVAL,  CADDLL },
		[TYPE_INTERVAL][TYPE_TIMESTAMP]  = { TYPE_TIMESTAMP, CADDLT },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CADDVV },
	},

	[OP_SUB] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CSUBII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CSUBID },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CSUBDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CSUBDD },

		// timestamp
		[TYPE_TIMESTAMP][TYPE_INTERVAL]  = { TYPE_TIMESTAMP, CSUBTL },
		[TYPE_TIMESTAMP][TYPE_TIMESTAMP] = { TYPE_INTERVAL,  CSUBTT },

		// interval
		[TYPE_INTERVAL][TYPE_INTERVAL]   = { TYPE_INTERVAL,  CSUBLL },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CSUBVV },
	},

	[OP_MUL] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CMULII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CMULID },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CMULDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CMULDD },

		// vector
		[TYPE_VECTOR][TYPE_VECTOR]       = { TYPE_VECTOR,    CMULVV },
	},

	[OP_DIV] =
	{
		// int
		[TYPE_INT][TYPE_INT]             = { TYPE_INT,       CDIVII },
		[TYPE_INT][TYPE_DOUBLE]          = { TYPE_DOUBLE,    CDIVID },

		// double
		[TYPE_DOUBLE][TYPE_INT]          = { TYPE_DOUBLE,    CDIVDI },
		[TYPE_DOUBLE][TYPE_DOUBLE]       = { TYPE_DOUBLE,    CDIVDD },
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
		[TYPE_STRING][TYPE_STRING]       = { TYPE_STRING,    CLIKESS },
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
			stmt_error(self->current, ast, "operation %s %s %s is not supported",
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
