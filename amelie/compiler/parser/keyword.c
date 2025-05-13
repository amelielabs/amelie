
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

Keyword keywords[] =
{
	// a
	{ KAND,                   "and",                   3  },
	{ KADD,                   "add",                   3  },
	{ KANY,                   "any",                   3  },
	{ KALL,                   "all",                   3  },
	{ KALTER,                 "alter",                 5  },
	{ KASC,                   "asc",                   3  },
	{ KAVG,                   "avg",                   3  },
	{ KAS,                    "as",                    2  },
	{ KAT,                    "at",                    2  },
	{ KALWAYS,                "always",                6  },

	// b
	{ KBEGIN,                 "begin",                 5  },
	{ KBETWEEN,               "between",               7  },
	{ KBY,                    "by",                    2  },

	// c
	{ KCURRENT_TIMESTAMP,     "current_timestamp",     17 },
	{ KCURRENT_DATE,          "current_date",          12 },
	{ KCAST,                  "cast",                  4  },
	{ KCALL,                  "call",                  4  },
	{ KCASE,                  "case",                  4  },
	{ KCONFLICT,              "conflict",              8  },
	{ KCOUNT,                 "count",                 5  },
	{ KCOMMIT,                "commit",                6  },
	{ KCREATE,                "create",                6  },
	{ KCHECKPOINT,            "checkpoint",            10 },
	{ KCASCADE,               "cascade",               7  },
	{ KCOLUMN ,               "column",                6  },

	// d
	{ KDELETE,                "delete",                6  },
	{ KDECLARE,               "declare",               7  },
	{ KDISTINCT,              "distinct",              8  },
	{ KDO,                    "do",                    2  },
	{ KDROP,                  "drop",                  4  },
	{ KDESC,                  "desc",                  4  },
	{ KDEBUG,                 "debug",                 5  },
	{ KDEFAULT,               "default",               7  },
	{ KDATE,                  "date",                  4  },

	// e
	{ KEXTRACT,               "extract",               7  },
	{ KEXECUTE,               "execute",               7  },
	{ KELSE,                  "else",                  4  },
	{ KEND,                   "end",                   3  },
	{ KERROR,                 "error",                 5  },
	{ KEXPLAIN,               "explain",               7  },
	{ KEXISTS,                "exists",                6  },
	{ KEXPIRE,                "expire",                6  },
	{ KEXTENDED,              "extended",              8  },

	// f
	{ KFALSE,                 "false",                 5  },
	{ KFROM,                  "from",                  4  },
	{ KFOR,                   "for",                   3  },
	{ KFORMAT,                "format",                6  },

	// g
	{ KGROUP,                 "group",                 5  },
	{ KGENERATE,              "generate",              8  },
	{ KGENERATED,             "generated",             9  },

	// h
	{ KHAVING,                "having",                6  },
	{ KHEAP,                  "heap",                  4  },

	// i
	{ KIS,                    "is",                    2  },
	{ KIN,                    "in",                    2  },
	{ KINSERT,                "insert",                6  },
	{ KINTO,                  "into",                  4  },
	{ KINTERVAL,              "interval",              8  },
	{ KINNER,                 "inner",                 5  },
	{ KIF,                    "if",                    2  },
	{ KINDEX,                 "index",                 5  },
	{ KINCLUDE,               "include",               7  },
	{ KIDENTITY,              "identity",              8  },

	// j
	{ KJOIN,                  "join",                  4  },

	// k
	{ KKEY,                   "key",                   3  },

	// l
	{ KLIKE,                  "like",                  4  },
	{ KLIMIT,                 "limit",                 5  },
	{ KLEFT,                  "left",                  4  },
	{ KLOCAL,                 "local",                 5  },
	{ KLOGGED,                "logged",                6  },

	// m
	{ KMIN,                   "min",                   3  },
	{ KMAX,                   "max",                   3  },

	// n
	{ KNULL,                  "null",                  4  },
	{ KNOT,                   "not",                   3  },
	{ KNOTHING,               "nothing",               7  },

	// o
	{ KOR,                    "or",                    2  },
	{ KON,                    "on",                    2  },
	{ KOFFSET,                "offset",                6  },
	{ KORDER,                 "order",                 5  },
	{ KOUTER,                 "outer",                 5  },

	// p
	{ KPROFILE,               "profile",               7  },
	{ KPROCEDURE,             "procedure",             9  },
	{ KPARTITIONS,            "partitions",            10 },
	{ KPOOL,                  "pool",                  4  },
	{ KPRIMARY,               "primary",               7  },

	// r
	{ KRIGHT,                 "right",                 5  },
	{ KRETURNING,             "returning",             9  },
	{ KRETURN,                "return",                6  },
	{ KREPLACE,               "replace",               7  },
	{ KRENAME,                "rename",                6  },
	{ KREPLICA,               "replica",               7  },
	{ KREPL,                  "repl",                  4  },
	{ KREPLICATION,           "replication",           11 },
	{ KRANDOM,                "random",                6  },
	{ KRESOLVE,               "resolve",               7  },
	{ KRESOLVED,              "resolved",              8  },

	// s
	{ KSELECT,                "select",                6  },
	{ KSELF,                  "self",                  4  },
	{ KSET,                   "set",                   3  },
	{ KSHOW,                  "show",                  4  },
	{ KSUM,                   "sum",                   3  },
	{ KSTART,                 "start",                 5  },
	{ KSTOP,                  "stop",                  4  },
	{ KSUBSCRIBE,             "subscribe",             9  },
	{ KSWITCH,                "switch",                6  },
	{ KSCHEMA,                "schema",                6  },
	{ KSECRET,                "secret",                6  },
	{ KSTORED,                "stored",                6  },

	// t
	{ KTRUE,                  "true",                  4  },
	{ KTIMESTAMP,             "timestamp",             9  },
	{ KTIMEZONE,              "timezone",              8  },
	{ KTHEN,                  "then",                  4  },
	{ KTO,                    "to",                    2  },
	{ KTABLE,                 "table",                 5  },
	{ KTRUNCATE,              "truncate",              8  },
	{ KTOKEN,                 "token",                 5  },

	// u
	{ KUPDATE,                "update",                6  },
	{ KUUID,                  "uuid",                  4  },
	{ KUNSET,                 "unset",                 5  },
	{ KUSE,                   "use",                   3  },
	{ KUSER,                  "user",                  4  },
	{ KUNIQUE,                "unique",                6  },
	{ KUSING,                 "using",                 5  },
	{ KURI,                   "uri",                   3  },
	{ KUNSUBSCRIBE,           "unsubscribe",           11 },
	{ KUNLOGGED,              "unlogged",              8  },

	// v
	{ KVALUES,                "values",                6  },
	{ KVECTOR,                "vector",                6  },
	{ KVIEW,                  "view",                  4  },

	// w
	{ KWHEN,                  "when",                  4  },
	{ KWHERE,                 "where",                 5  },
	{ KWITH,                  "with",                  4  },
	{ KWATCH,                 "watch",                 5  },
	{ KWORKERS,               "workers",               7  },

	{ 0,                       NULL,                   0  }
};

Keyword* keywords_alpha[26] =
{
	&keywords[0],
	&keywords[KBEGIN - KKEYWORD - 1],
	&keywords[KCURRENT_TIMESTAMP - KKEYWORD - 1],
	&keywords[KDELETE - KKEYWORD - 1],
	&keywords[KEXTRACT - KKEYWORD - 1],
	&keywords[KFALSE - KKEYWORD - 1],
	&keywords[KGROUP - KKEYWORD - 1],
	&keywords[KHAVING - KKEYWORD - 1],
	&keywords[KIS - KKEYWORD - 1],
	&keywords[KJOIN - KKEYWORD - 1],
	&keywords[KKEY - KKEYWORD - 1],
	&keywords[KLIKE - KKEYWORD - 1],
	&keywords[KMIN - KKEYWORD - 1],
	&keywords[KNULL - KKEYWORD - 1],
	&keywords[KOR - KKEYWORD - 1],
	&keywords[KPROFILE - KKEYWORD - 1],
	NULL,
	&keywords[KRIGHT - KKEYWORD - 1],
	&keywords[KSELECT - KKEYWORD - 1],
	&keywords[KTRUE - KKEYWORD - 1],
	&keywords[KUPDATE - KKEYWORD - 1],
	&keywords[KVALUES - KKEYWORD - 1],
	&keywords[KWHEN - KKEYWORD - 1],
	NULL,
	NULL,
	NULL
};
