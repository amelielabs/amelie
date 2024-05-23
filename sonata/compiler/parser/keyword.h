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
	KREAL,
	KINT,
	KARGUMENT,
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
	KNAME_COMPOUND_STAR_STAR,
	KKEYWORD,

	// keywords
	KTRUE,
	KFALSE,
	KNULL,
	KAND,
	KOR,
	KNOT,

	KCALL,
	KMETHOD,
	KNEG,
	KARRAY,
	KSTAR,
	KSTAR_STAR,

	KTMAP,
	KTARRAY,
	KTINT,
	KTINTEGER,
	KTBOOL,
	KTBOOLEAN,
	KTREAL,
	KTSTRING,
	KTEXT,
	KTOBJECT,
	KJSON,

	KSET,
	KUNSET,
	KADD,

	KEXPLAIN,
	KPROFILE,

	KTO,
	KSHOW,

	KCHECKPOINT,
	KWORKERS,

	KREPL,
	KREPLICATION,
	KPROMOTE,
	KEXEC,

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
	KWITH,

	KINSERT,
	KREPLACE,
	KINTO,
	KVALUES,
	KGENERATE,
	KGENERATED,
	KSERIAL,

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
