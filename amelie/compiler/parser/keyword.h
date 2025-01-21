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

	// name/path
	KNAME,
	KNAME_COMPOUND,
	KNAME_COMPOUND_STAR,
	KSTAR,

	// meta
	KARGS,
	KCALL,
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
	KCASE,
	KCONFLICT,
	KCOUNT,
	KCOMMIT,
	KCREATE,
	KCHECKPOINT,
	KCASCADE,
	KCOMPUTE,
	KCOLUMN,

	// d
	KDELETE,
	KDISTINCT,
	KDO,
	KDROP,
	KDESC,
	KDEBUG,
	KDISTRIBUTED,
	KDEFAULT,
	KDATE,

	// e
	KEXTRACT,
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
	KFUNCTION,
	KFOR,
	KFORMAT,

	// g
	KGROUP,
	KGENERATE,
	KGENERATED,

	// h
	KHAVING,

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
	KNODE,

	// o
	KOR,
	KON,
	KOFFSET,
	KORDER,
	KOUTER,

	// p
	KPROFILE,
	KPRIMARY,

	// q

	// r
	KRIGHT,
	KRETURNING,
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
	KSHARED,
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
