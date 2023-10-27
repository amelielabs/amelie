
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static Keyword keywords_a[] =
{
	{ KAND,                   "and",                   3  },
	{ KADD,                   "add",                   3  },
	{ KALTER,                 "alter",                 5  },
	{ KASC,                   "asc",                   3  },
	{ KAVG,                   "avg",                   3  },
	{ KAS,                    "as",                    2  },
	{ KTARRAY,                "array",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_b[] =
{
	{ KBEGIN,                 "begin",                 5  },
	{ KBY,                    "by",                    2  },
	{ KTBOOL,                 "bool",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_c[] =
{
	{ KCOMMIT,                "commit",                6  },
	{ KCOUNT,                 "count",                 5  },
	{ KCREATE,                "create",                6  },
	{ KCHECKPOINT,            "checkpoint",            10 },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_d[] =
{
	{ KDELETE,                "delete",                6  },
	{ KDROP,                  "drop",                  4  },
	{ KDESC,                  "desc",                  4  },
	{ KDEBUG,                 "debug",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_e[] =
{
	{ KEXPLAIN,               "explain",               7  },
	{ KEXISTS,                "exists",                6  },
	{ KEXEC,                  "exec",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_f[] =
{
	{ KFALSE,                 "false",                 5  },
	{ KFROM,                  "from",                  4  },
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
	{ KINSERT,                "insert",                6  },
	{ KINTO,                  "into",                  4  },
	{ KTINT,                  "int",                   3  },
	{ KIF,                    "if",                    2  },
	{ KINDEX,                 "index",                 5  },
	{ KINCLUDE,               "include",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_j[] =
{
	{ KJSON,                  "json",                  4  },
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
	{ KLIMIT,                 "limit",                 5  },
	{ KLOCAL,                 "local",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_m[] =
{
	{ KTMAP,                  "map",                   3  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_n[] =
{
	{ KNULL,                  "null",                  4  },
	{ KNOT,                   "not",                   3  },
	{ KNODE,                  "node",                  4  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_o[] =
{
	{ KOR,                    "or",                    2  },
	{ KON,                    "on",                    2  },
	{ KOFFSET,                "offset",                6  },
	{ KORDER,                 "order",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_p[] =
{
	{ KPROFILE,               "profile",               7  },
	{ KPASSWORD,              "password",              8  },
	{ KPRIMARY,               "primary",               7  },
	{ KPROMOTE,               "promote",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_q[] =
{
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_r[] =
{
	{ KTREAL,                 "real",                  4  },
	{ KREPLACE,               "replace",               7  },
	{ KROLLBACK,              "rollback",              8  },
	{ KRENAME,                "rename",                6  },
	{ KREPL,                  "repl",                  4  },
	{ KREPLICATION,           "replication",           11 },
	{ KREFERENCE,             "reference",             9  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_s[] =
{
	{ KSELECT,                "select",                6  },
	{ KSET,                   "set",                   3  },
	{ KSHOW,                  "show",                  4  },
	{ KTSTRING,               "string",                6  },
	{ KSUM,                   "sum",                   3  },
	{ KSTART,                 "start",                 5  },
	{ KSTOP,                  "stop",                  4  },
	{ KSWITCH,                "switch",                6  },
	{ KSCHEMA,                "schema",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_t[] =
{
	{ KTRUE,                  "true",                  4  },
	{ KTO,                    "to",                    2  },
	{ KTABLE,                 "table",                 5  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_u[] =
{
	{ KUPDATE,                "update",                6  },
	{ KUNSET,                 "unset",                 5  },
	{ KUSER,                  "user",                  4  },
	{ KUNIQUE,                "unique",                6  },
	{ KUSING,                 "using",                 5  },
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
	{ KWHERE,                 "where",                 5  },
	{ KWITH,                  "with",                  4  },
	{ KWAIT,                  "wait",                  4  },
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
