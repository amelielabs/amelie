
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static Keyword keywords_a[] =
{
	{ KAND,                   "and",                   3  },
	{ KADD,                   "add",                   3  },
	{ KANY,                   "any",                   3  },
	{ KALL,                   "all",                   3  },
	{ KALTER,                 "alter",                 5  },
	{ KASC,                   "asc",                   3  },
	{ KAVG,                   "avg",                   3  },
	{ KAS,                    "as",                    2  },
	{ KAT,                    "at",                    2  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_b[] =
{
	{ KBEGIN,                 "begin",                 5  },
	{ KBETWEEN,               "between",               7  },
	{ KBY,                    "by",                    2  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_c[] =
{
	{ KCURRENT_TIMESTAMP,     "current_timestamp",     17 },
	{ KCASE,                  "case",                  4  },
	{ KCONFLICT,              "conflict",              8  },
	{ KCOUNT,                 "count",                 5  },
	{ KCOMMIT,                "commit",                6  },
	{ KCREATE,                "create",                6  },
	{ KCHECKPOINT,            "checkpoint",            10 },
	{ KCASCADE,               "cascade",               7  },
	{ KCOMPUTE,               "compute",               7  },
	{ KCOLUMN ,               "column",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_d[] =
{
	{ KDELETE,                "delete",                6  },
	{ KDISTINCT,              "distinct",              8  },
	{ KDO,                    "do",                    2  },
	{ KDROP,                  "drop",                  4  },
	{ KDESC,                  "desc",                  4  },
	{ KDEBUG,                 "debug",                 5  },
	{ KDISTRIBUTED,           "distributed",           11 },
	{ KDEFAULT,               "default",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_e[] =
{
	{ KEXECUTE,               "execute",               7  },
	{ KEXTRACT,               "extract",               7  },
	{ KELSE,                  "else",                  4  },
	{ KEND,                   "end",                   3  },
	{ KEXPLAIN,               "explain",               7  },
	{ KEXISTS,                "exists",                6  },
	{ KEXPIRE,                "expire",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_f[] =
{
	{ KFALSE,                 "false",                 5  },
	{ KFROM,                  "from",                  4  },
	{ KFUNCTION,              "function",              8  },
	{ KFOR,                   "for",                   3  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_g[] =
{
	{ KGROUP,                 "group",                 5  },
	{ KGENERATE,              "generate",              8  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_h[] =
{
	{ KHAVING,                "having",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_i[] =
{
	{ KIS,                    "is",                    2  },
	{ KIN,                    "in",                    2  },
	{ KINSERT,                "insert",                6  },
	{ KINTO,                  "into",                  4  },
	{ KINTERVAL,              "interval",              8  },
	{ KINNER,                 "inner",                 5  },
	{ KIF,                    "if",                    2  },
	{ KINDEX,                 "index",                 5  },
	{ KINCLUDE,               "include",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_j[] =
{
	{ KJOIN,                  "join",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_k[] =
{
	{ KKEY,                   "key",                   3  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_l[] =
{
	{ KLIKE,                  "like",                  4  },
	{ KLIMIT,                 "limit",                 5  },
	{ KLAMBDA,                "lambda",                6  },
	{ KLEFT,                  "left",                  4  },
	{ KLOCAL,                 "local",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_m[] =
{
	{ KMIN,                   "min",                   3  },
	{ KMAX,                   "max",                   3  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_n[] =
{
	{ KNULL,                  "null",                  4  },
	{ KNOT,                   "not",                   3  },
	{ KNOTHING,               "nothing",               7  },
	{ KNODE,                  "node",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_o[] =
{
	{ KOR,                    "or",                    2  },
	{ KON,                    "on",                    2  },
	{ KOFFSET,                "offset",                6  },
	{ KORDER,                 "order",                 5  },
	{ KOUTER,                 "outer",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_p[] =
{
	{ KPROFILE,               "profile",               7  },
	{ KPRIMARY,               "primary",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_q[] =
{
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_r[] =
{
	{ KRIGHT,                 "right",                 5  },
	{ KRETURN,                "return",                6  },
	{ KRETURNING,             "returning",             9  },
	{ KREPLACE,               "replace",               7  },
	{ KRENAME,                "rename",                6  },
	{ KREPLICA,               "replica",               7  },
	{ KREPL,                  "repl",                  4  },
	{ KREPLICATION,           "replication",           11 },
	{ KRANDOM,                "random",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_s[] =
{
	{ KSELECT,                "select",                6  },
	{ KSET,                   "set",                   3  },
	{ KSHOW,                  "show",                  4  },
	{ KSUM,                   "sum",                   3  },
	{ KSTART,                 "start",                 5  },
	{ KSTOP,                  "stop",                  4  },
	{ KSUBSCRIBE,             "subscribe",             9  },
	{ KSWITCH,                "switch",                6  },
	{ KSCHEMA,                "schema",                6  },
	{ KSERIAL,                "serial",                6  },
	{ KSHARED,                "shared",                6  },
	{ KSECRET,                "secret",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_t[] =
{
	{ KTRUE,                  "true",                  4  },
	{ KTIMESTAMP,             "timestamp",             9  },
	{ KTIMEZONE,              "timezone",              8  },
	{ KTHEN,                  "then",                  4  },
	{ KTO,                    "to",                    2  },
	{ KTABLE,                 "table",                 5  },
	{ KTRUNCATE,              "truncate",              8  },
	{ KTOKEN,                 "token",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_u[] =
{
	{ KUPDATE,                "update",                6  },
	{ KUNSET,                 "unset",                 5  },
	{ KUSE,                   "use",                   3  },
	{ KUSER,                  "user",                  4  },
	{ KUNIQUE,                "unique",                6  },
	{ KUSING,                 "using",                 5  },
	{ KURI,                   "uri",                   3  },
	{ KUNSUBSCRIBE,           "unsubscribe",           11 },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_v[] =
{
	{ KVALUES,                "values",                6  },
	{ KVIEW,                  "view",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_w[] =
{
	{ KWHEN,                  "when",                  4  },
	{ KWHERE,                 "where",                 5  },
	{ KWITH,                  "with",                  4  },
	{ KWATCH,                 "watch",                 5  },
	{ KWORKERS,               "workers",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_x[] =
{
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_y[] =
{
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_z[] =
{
	{ 0,                       NULL,                   0  }
};

Keyword* keywords[26] =
{
	keywords_a,
	keywords_b,
	keywords_c,
	keywords_d,
	keywords_e,
	keywords_f,
	keywords_g,
	keywords_h,
	keywords_i,
	keywords_j,
	keywords_k,
	keywords_l,
	keywords_m,
	keywords_n,
	keywords_o,
	keywords_p,
	keywords_q,
	keywords_r,
	keywords_s,
	keywords_t,
	keywords_u,
	keywords_v,
	keywords_w,
	keywords_x,
	keywords_y,
	keywords_z
};
