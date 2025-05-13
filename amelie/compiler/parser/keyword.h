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

typedef struct Keyword Keyword;

enum
{
	// lexer
	KEOF = 128,

	// consts
	KREAL,
	KINT,
	KSTRING,
	KARGID,

	// lexer operations
	KSHL,
	KSHR,
	KLTE,
	KGTE,
	KNEQU,
	KCAT,
	KARROW,
	KMETHOD,
	KASSIGN,

	// name/path
	KNAME,
	KNAME_COMPOUND,
	KNAME_COMPOUND_STAR,
	KSTAR,

	// meta
	KARGS,
	KFUNC,
	KNEG,
	KARRAY,
	KAGGR,
	KAGGRKEY,

	// keywords
	KKEYWORD,

	// a
	KAND,
	KADD,
	KANY,
	KALL,
	KALTER,
	KASC,
	KAVG,
	KAS,
	KAT,
	KALWAYS,

	// b
	KBEGIN,
	KBETWEEN,
	KBY,

	// c
	KCURRENT_TIMESTAMP,
	KCURRENT_DATE,
	KCAST,
	KCALL,
	KCASE,
	KCONFLICT,
	KCOUNT,
	KCOMMIT,
	KCREATE,
	KCHECKPOINT,
	KCASCADE,
	KCOLUMN,

	// d
	KDELETE,
	KDECLARE,
	KDISTINCT,
	KDO,
	KDROP,
	KDESC,
	KDEBUG,
	KDEFAULT,
	KDATE,

	// e
	KEXTRACT,
	KEXECUTE,
	KELSE,
	KEND,
	KERROR,
	KEXPLAIN,
	KEXISTS,
	KEXPIRE,
	KEXTENDED,

	// f
	KFALSE,
	KFROM,
	KFOR,
	KFORMAT,

	// g
	KGROUP,
	KGENERATE,
	KGENERATED,

	// h
	KHAVING,
	KHEAP,

	// i
	KIS,
	KIN,
	KINSERT,
	KINTO,
	KINTERVAL,
	KINNER,
	KIF,
	KINDEX,
	KINCLUDE,
	KIDENTITY,

	// j
	KJOIN,

	// k
	KKEY,

	// l
	KLIKE,
	KLIMIT,
	KLEFT,
	KLOCAL,
	KLOGGED,

	// m
	KMIN,
	KMAX,

	// n
	KNULL,
	KNOT,
	KNOTHING,

	// o
	KOR,
	KON,
	KOFFSET,
	KORDER,
	KOUTER,

	// p
	KPROFILE,
	KPROCEDURE,
	KPARTITIONS,
	KPOOL,
	KPRIMARY,

	// q

	// r
	KRIGHT,
	KRETURNING,
	KRETURN,
	KREPLACE,
	KRENAME,
	KREPLICA,
	KREPL,
	KREPLICATION,
	KRANDOM,
	KRESOLVE,
	KRESOLVED,

	// s
	KSELECT,
	KSELF,
	KSET,
	KSHOW,
	KSUM,
	KSTART,
	KSTOP,
	KSUBSCRIBE,
	KSWITCH,
	KSCHEMA,
	KSECRET,
	KSTORED,

	// t
	KTRUE,
	KTIMESTAMP,
	KTIMEZONE,
	KTHEN,
	KTO,
	KTABLE,
	KTRUNCATE,
	KTOKEN,

	// u
	KUPDATE,
	KUUID,
	KUNSET,
	KUSE,
	KUSER,
	KUNIQUE,
	KUSING,
	KURI,
	KUNSUBSCRIBE,
	KUNLOGGED,

	// v
	KVALUES,
	KVECTOR,
	KVIEW,

	// w
	KWHEN,
	KWHERE,
	KWITH,
	KWATCH,
	KWORKERS,

	// x
	// y
	// z

	KEYWORD_MAX
};

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

extern Keyword  keywords[];
extern Keyword* keywords_alpha[];
