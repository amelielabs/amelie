
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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
	{ KBY,                    "by",                    2  },
	{ KTBOOL,                 "bool",                  4  },
	{ KTBOOLEAN,              "boolean",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_c[] =
{
	{ KCONFLICT,              "conflict",              8  },
	{ KCOUNT,                 "count",                 5  },
	{ KCREATE,                "create",                6  },
	{ KCHECKPOINT,            "checkpoint",            10 },
	{ KCASCADE,               "cascade",               7  },
	{ KCOMPUTE,               "compute",               7  },
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
	{ KDEFAULT,               "default",               7  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_e[] =
{
	{ KEXPLAIN,               "explain",               7  },
	{ KEXISTS,                "exists",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_f[] =
{
	{ KFALSE,                 "false",                 5  },
	{ KFROM,                  "from",                  4  },
	{ KFOR,                   "for",                   3  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_g[] =
{
	{ KGROUP,                 "group",                 5  },
	{ KGENERATE,              "generate",              8  },
	{ KGENERATED,             "generated",             9  },
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
	{ KTINTEGER,              "integer",               7  },
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
	{ KMIN,                   "min",                   3  },
	{ KMAX,                   "max",                   3  },
	{ KTMAP,                  "map",                   3  },
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
	{ KTOBJECT,               "object",                6  },
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
	{ KRENAME,                "rename",                6  },
	{ KREPLICA,               "replica",               7  },
	{ KREPL,                  "repl",                  4  },
	{ KREPLICATION,           "replication",           11 },
	{ KREFERENCE,             "reference",             9  },
	{ KRESET,                 "reset",                 5  },
	{ KRANDOM,                "random",                6  },
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
	{ KSERIAL,                "serial",                6  },
	{ 0,                       NULL,                   0  }
};

static Keyword keywords_t[] =
{
	{ KTRUE,                  "true",                  4  },
	{ KTO,                    "to",                    2  },
	{ KTABLE,                 "table",                 5  },
	{ KTEXT,                  "text",                  4  },
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
