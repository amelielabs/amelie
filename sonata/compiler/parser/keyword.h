#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	KINTERVAL,
	KTIMESTAMP,
	KTIMESTAMPTZ,
	KARGUMENT,

	// lexer operations
	KSHL,
	KSHR,
	KLTE,
	KGTE,
	KNEQU,
	KCAT,

	// name/path
	KNAME,
	KNAME_COMPOUND,
	KNAME_COMPOUND_STAR,
	KNAME_COMPOUND_STAR_STAR,
	KKEYWORD,

	// keywords
	KTRUE,
	KFALSE,
	KNULL,
	KAND,
	KOR,
	KNOT,

	KARGS,
	KCALL,
	KMETHOD,
	KARROW,
	KNEG,
	KARRAY,
	KSTAR,
	KSTAR_STAR,

	KSET,
	KUNSET,
	KADD,

	KEXPLAIN,
	KPROFILE,

	KTO,
	KSHOW,

	KCHECKPOINT,
	KWORKERS,

	KREPLICA,
	KREPL,
	KREPLICATION,
	KPROMOTE,
	KRESET,

	KCREATE,
	KUSER,
	KNODE,
	KURI,
	KFOR,
	KCOMPUTE,
	KPASSWORD,
	KSCHEMA,
	KTABLE,
	KLOCAL,
	KINDEX,
	KUSE,
	KVIEW,
	KUNIQUE,
	KPRIMARY,
	KKEY,
	KDEFAULT,
	KREFERENCE,
	KINCLUDE,
	KON,
	KIF,
	KEXISTS,
	KDROP,
	KCASCADE,
	KALTER,
	KRENAME,
	KCOLUMN,
	KWITH,

	KINSERT,
	KINTO,
	KVALUES,
	KGENERATE,
	KSERIAL,
	KRANDOM,

	KWHERE,
	KHAVING,
	KLIMIT,
	KOFFSET,
	KGROUP,
	KORDER,
	KBY,
	KASC,
	KDESC,
	KDISTINCT,
	KJOIN,

	KCOUNT,
	KSUM,
	KAVG,
	KMIN,
	KMAX,
	KAGGR,

	KDELETE,
	KFROM,
	KAS,
	KUPDATE,
	KCONFLICT,
	KDO,
	KNOTHING,
	KSELECT,
	KWATCH,

	KSTART,
	KSTOP,
	KSWITCH,
	KUSING,
	KDEBUG
};

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

extern Keyword* keywords[26];
