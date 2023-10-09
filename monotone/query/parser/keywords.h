#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Keyword Keyword;

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

enum
{
	// lexer
	KEOF = 128,
	KFLOAT,
	KINT,
	KARGUMENT,
	KARGUMENT_NAME,
	KSTRING,
	KSHL,
	KSHR,
	KLTE,
	KGTE,
	KNEQU,
	KCAT,
	KNAME,
	KNAME_COMPOUND,
	KNAME_COMPOUND_STAR,
	KKEYWORD,

	// keywords
	KTRUE,
	KFALSE,
	KNULL,
	KAND,
	KOR,
	KNOT,

	KCALL,
	KNEG,
	KIDX,
	KSTAR,

	KTMAP,
	KTARRAY,
	KTINT,
	KTBOOL,
	KTFLOAT,
	KTSTRING,
	KJSON,

	KSET,
	KUNSET,
	KHAS,
	KADD,

	KSIZEOF,

	KEXPLAIN,
	KPROFILE,
	KTO,
	KSHOW,

	KCHECKPOINT,

	KREPL,
	KREPLICATION,
	KPROMOTE,
	KEXEC,

	KBEGIN,
	KCOMMIT,
	KROLLBACK,

	KCREATE_USER,
	KCREATE_TABLE,
	KDROP_USER,
	KDROP_TABLE,

	KCREATE,
	KUSER,
	KNODE,
	KPASSWORD,
	KTABLE,
	KLOCAL,
	KINDEX,
	KVIEW,
	KUNIQUE,
	KPRIMARY,
	KKEY,
	KREFERENCE,
	KINCLUDE,
	KON,
	KIF,
	KEXISTS,
	KDROP,
	KALTER,
	KRENAME,
	KWITH,

	KINSERT,
	KREPLACE,
	KINTO,
	KVALUES,
	KGENERATE,

	KWHERE,
	KHAVING,
	KLIMIT,
	KOFFSET,
	KGROUP,
	KORDER,
	KBY,
	KASC,
	KDESC,
	KJOIN,

	KCOUNT,
	KSUM,
	KAVG,

	KDELETE,
	KFROM,
	KAS,
	KUPDATE,
	KSELECT,
	KWAIT,

	KSTART,
	KSTOP,
	KSWITCH,
	KUSING,
	KDEBUG
};

extern Keyword* keywords[26];
