
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
#include <amelie_data.h>
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

OpExpr op_bor[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBORII },
};

OpExpr op_band[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBANDII },
};

OpExpr op_bxor[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBXORII },
};

OpExpr op_bshl[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBSHLII },
};

OpExpr op_bshr[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT, CBSHRII },
};

OpExpr op_equ[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_gte[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_gt[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_lte[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_lt[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_add[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_sub[VALUE_MAX][VALUE_MAX] =
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
};

OpExpr op_mul[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CMULII },
	[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CMULID },

	// double
	[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CMULDI },
	[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CMULDD },

	// vector
	[VALUE_VECTOR][VALUE_VECTOR]       = { VALUE_VECTOR,    CMULVV },
};

OpExpr op_div[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CDIVII },
	[VALUE_INT][VALUE_DOUBLE]          = { VALUE_DOUBLE,    CDIVID },

	// double
	[VALUE_DOUBLE][VALUE_INT]          = { VALUE_DOUBLE,    CDIVDI },
	[VALUE_DOUBLE][VALUE_DOUBLE]       = { VALUE_DOUBLE,    CDIVDD },
};

OpExpr op_mod[VALUE_MAX][VALUE_MAX] =
{
	// int
	[VALUE_INT][VALUE_INT]             = { VALUE_INT,       CMODII },
};

OpExpr op_cat[VALUE_MAX][VALUE_MAX] =
{
	// string
	[VALUE_STRING][VALUE_STRING]       = { VALUE_STRING,    CCATSS },
};

OpExpr op_idx[VALUE_MAX][VALUE_MAX] =
{
	// json
	[VALUE_JSON][VALUE_STRING]         = { VALUE_JSON,      CIDXJS },
	[VALUE_JSON][VALUE_INT]            = { VALUE_JSON,      CIDXJI },

	// vector
	[VALUE_VECTOR][VALUE_INT]          = { VALUE_DOUBLE,    CIDXVI },
};

OpExpr op_like[VALUE_MAX][VALUE_MAX] =
{
	// string
	[VALUE_STRING][VALUE_STRING]       = { VALUE_STRING,    CLIKESS },
};

OpExpr op_cursor[CURSOR_MAX][TYPE_MAX] =
{
	[CURSOR_TABLE][TYPE_INT8]      = { VALUE_INT,       CCURSOR_READI8  },
	[CURSOR_TABLE][TYPE_INT16]     = { VALUE_INT,       CCURSOR_READI16 },
	[CURSOR_TABLE][TYPE_INT32]     = { VALUE_INT,       CCURSOR_READI32 },
	[CURSOR_TABLE][TYPE_INT64]     = { VALUE_INT,       CCURSOR_READI64 },
	[CURSOR_TABLE][TYPE_FLOAT]     = { VALUE_DOUBLE,    CCURSOR_READF   },
	[CURSOR_TABLE][TYPE_DOUBLE]    = { VALUE_DOUBLE,    CCURSOR_READD   },
	[CURSOR_TABLE][TYPE_TIMESTAMP] = { VALUE_TIMESTAMP, CCURSOR_READT   },
	[CURSOR_TABLE][TYPE_INTERVAL]  = { VALUE_INTERVAL,  CCURSOR_READL   },
	[CURSOR_TABLE][TYPE_TEXT]      = { VALUE_STRING,    CCURSOR_READS   },
	[CURSOR_TABLE][TYPE_JSON]      = { VALUE_JSON,      CCURSOR_READJ   },
	[CURSOR_TABLE][TYPE_VECTOR]    = { VALUE_VECTOR,    CCURSOR_READV   },
};
