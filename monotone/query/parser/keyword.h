#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Keyword Keyword;

enum
{
	// lexer
	KEOF = 128,
	KREAL,
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
	KARRAY,
	KSTAR,

	KTMAP,
	KTARRAY,
	KTINT,
	KTBOOL,
	KTREAL,
	KTSTRING,
	KJSON,

	KSET,
	KUNSET,
	KADD,

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

	KCREATE,
	KUSER,
	KNODE,
	KPASSWORD,
	KSCHEMA,
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
	KCASCADE,
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

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

extern Keyword* keywords[26];
