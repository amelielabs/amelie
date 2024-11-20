
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
#include <amelie_value.h>
#include <amelie_store.h>
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
cast_op[OP_MAX][VALUE_MAX][VALUE_MAX] =
{
	[OP_EQU] =
	{
		// bool
		[VALUE_BOOL][VALUE_BOOL]           = { VALUE_BOOL, CEQUII },
		[VALUE_BOOL][VALUE_INT]            = { VALUE_BOOL, CEQUII },

		// int
		[VALUE_INT][VALUE_BOOL]            = { VALUE_BOOL, CEQUII },
		[VALUE_INT][VALUE_INT]             = { VALUE_BOOL, CEQUII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_BOOL, CEQUID },
		[VALUE_INT][VALUE_TIMESTAMP]       = { VALUE_BOOL, CEQUII },

		// double
		[VALUE_DOUBLE][VALUE_BOOL]         = { VALUE_BOOL, CEQUDI },
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_BOOL, CEQUDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_BOOL, CEQUDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INT]       = { VALUE_BOOL, CEQUII },
		[VALUE_TIMESTAMP][VALUE_TIMESTAMP] = { VALUE_BOOL, CEQUII },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_BOOL, CEQULL },

		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_BOOL, CEQUSS },

		// json
		[VALUE_JSON][VALUE_JSON]           = { VALUE_BOOL, CEQUJJ },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_BOOL, CEQUVV },
	},

	[OP_GTE] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_BOOL, CGTEII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_BOOL, CGTEID },
		[VALUE_INT][VALUE_TIMESTAMP]       = { VALUE_BOOL, CGTEII },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_BOOL, CGTEDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_BOOL, CGTEDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INT]       = { VALUE_BOOL, CGTEII },
		[VALUE_TIMESTAMP][VALUE_TIMESTAMP] = { VALUE_BOOL, CGTEII },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_BOOL, CGTELL },

		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_BOOL, CGTESS },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_BOOL, CGTEVV },
	},

	[OP_GT] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_BOOL, CGTII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_BOOL, CGTID },
		[VALUE_INT][VALUE_TIMESTAMP]       = { VALUE_BOOL, CGTII },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_BOOL, CGTDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_BOOL, CGTDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INT]       = { VALUE_BOOL, CGTII },
		[VALUE_TIMESTAMP][VALUE_TIMESTAMP] = { VALUE_BOOL, CGTII },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_BOOL, CGTLL },

		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_BOOL, CGTSS },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_BOOL, CGTVV }
	},

	[OP_LTE] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_BOOL, CLTEII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_BOOL, CLTEID },
		[VALUE_INT][VALUE_TIMESTAMP]       = { VALUE_BOOL, CLTEII },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_BOOL, CLTEDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_BOOL, CLTEDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INT]       = { VALUE_BOOL, CLTEII },
		[VALUE_TIMESTAMP][VALUE_TIMESTAMP] = { VALUE_BOOL, CLTEII },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_BOOL, CLTELL },

		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_BOOL, CLTESS },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_BOOL, CLTEVV },
	},

	[OP_LT] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_BOOL, CLTII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_BOOL, CLTID },
		[VALUE_INT][VALUE_TIMESTAMP]       = { VALUE_BOOL, CLTII },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_BOOL, CLTDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_BOOL, CLTDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INT]       = { VALUE_BOOL, CLTII },
		[VALUE_TIMESTAMP][VALUE_TIMESTAMP] = { VALUE_BOOL, CLTII },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_BOOL, CLTLL },

		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_BOOL, CLTSS },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_BOOL, CLTVV },
	},

	[OP_ADD] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CADDII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CADDID },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CADDDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CADDDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INTERVAL]  = { VALUE_TIMESTAMP, CADDTL },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_INTERVAL,  CADDLL },
		[VALUE_INTERVAL][VALUE_TIMESTAMP]  = { VALUE_TIMESTAMP, CADDLT },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_VECTOR,    CADDVV },
	},

	[OP_SUB] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CSUBII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CSUBID },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CSUBDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CSUBDD },

		// timestamp
		[VALUE_TIMESTAMP][VALUE_INTERVAL]  = { VALUE_TIMESTAMP, CSUBTL },

		// interval
		[VALUE_INTERVAL][VALUE_INTERVAL]   = { VALUE_INTERVAL,  CSUBLL },
		[VALUE_INTERVAL][VALUE_TIMESTAMP]  = { VALUE_TIMESTAMP, CSUBLT },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_VECTOR,    CSUBVV },
	},

	[OP_MUL] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CMULII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CMULID },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CMULDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CMULDD },

		// vector
		[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_VECTOR,    CMULVV },
	},

	[OP_DIV] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CDIVII },
		[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CDIVID },

		// double
		[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CDIVDI },
		[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CDIVDD },
	},

	[OP_MOD] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CMODII },
	},

	[OP_CAT] =
	{
		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_STRING,    CCATSS },
	},

	[OP_IDX] =
	{
		// json
		[VALUE_JSON][VALUE_STRING]         = { VALUE_JSON,      CIDXJS },
		[VALUE_JSON][VALUE_INT]            = { VALUE_JSON,      CIDXJI },

		// vector
		[VALUE_VECTOR][VALUE_INT]          = { VALUE_DOUBLE,    CIDXVI },
	},

	[OP_LIKE] =
	{
		// string
		[VALUE_STRING][VALUE_STRING]       = { VALUE_STRING,    CLIKESS },
	},

	[OP_BOR] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBORII },
	},

	[OP_BAND] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBANDII },
	},

	[OP_BXOR] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBXORII },
	},

	[OP_BSHL] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBSHLII },
	},

	[OP_BSHR] =
	{
		// int
		[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBSHRII },
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
	[OP_LIKE] = "LIKE",
	[OP_BOR]  = "|",
	[OP_BAND] = "&",
	[OP_BXOR] = "^",
	[OP_BSHL] = "<<",
	[OP_BSHR] = ">>"
};

hot int
cast_operator(Compiler* self, int op, int l, int r)
{
	assert(op < OP_MAX);
	auto lt   = rtype(self, l);
	auto rt   = rtype(self, r);
	auto cast = &cast_op[op][lt][rt];
	if (unlikely(cast->type == VALUE_NULL))
	{
		error("unsupported operation: <%s> %s <%s>",
		      value_typeof(lt), cast_op_names[op],
		      value_typeof(rt));
	}
	int rc;
	rc = op3(self, cast->op, rpin(self, cast->type), l, r);
	runpin(self, l);
	runpin(self, r);
	return rc;
}
