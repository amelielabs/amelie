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

typedef struct OpDesc OpDesc;

enum
{
	// control
	CRET,
	CNOP,
	CJMP,
	CJTR,
	CJNTR,
	CJGTED,
	CJLTD,

	// values
	CFREE,
	CDUP,
	CSWAP,

	// stack
	CPUSH,
	CPOP,

	// consts
	CNULL,
	CBOOL,
	CINT,
	CDOUBLE,
	CSTRING,
	CJSON,
	CJSON_OBJ,
	CJSON_ARRAY,
	CINTERVAL,
	CTIMESTAMP,
	CVECTOR,
	CUUID,

	// argument
	CARG,
	CEXCLUDED,

	// null operations
	CNULLOP,
	CIS,

	// logical (any)
	CAND,
	COR,
	CNOT,

	// bitwise operations
	CBORII,
	CBANDII,
	CBXORII,
	CBSHLII,
	CBSHRII,
	CBINVI,

	// equ
	CEQUII,
	CEQUID,
	CEQUDI,
	CEQUDD,
	CEQULL,
	CEQUSS,
	CEQUJJ,
	CEQUVV,
	CEQUUU,

	// gte
	CGTEII,
	CGTEID,
	CGTEDI,
	CGTEDD,
	CGTELL,
	CGTESS,
	CGTEVV,
	CGTEUU,

	// gt
	CGTII,
	CGTID,
	CGTDI,
	CGTDD,
	CGTLL,
	CGTSS,
	CGTVV,
	CGTUU,

	// lte
	CLTEII,
	CLTEID,
	CLTEDI,
	CLTEDD,
	CLTELL,
	CLTESS,
	CLTEVV,
	CLTEUU,

	// lt
	CLTII,
	CLTID,
	CLTDI,
	CLTDD,
	CLTLL,
	CLTSS,
	CLTVV,
	CLTUU,

	// add
	CADDII,
	CADDID,
	CADDDI,
	CADDDD,
	CADDTL,
	CADDLL,
	CADDLT,
	CADDVV,

	// sub
	CSUBII,
	CSUBID,
	CSUBDI,
	CSUBDD,
	CSUBTL,
	CSUBLL,
	CSUBVV,

	// mul
	CMULII,
	CMULID,
	CMULDI,
	CMULDD,
	CMULVV,

	// div
	CDIVII,
	CDIVID,
	CDIVDI,
	CDIVDD,

	// mod
	CMODII,

	// neg
	CNEGI,
	CNEGD,

	// cat
	CCATSS,

	// idx
	CIDXJS,
	CIDXJI,
	CIDXVI,

	// dot
	CDOTJS,

	// like
	CLIKESS,

	// set operations
	CIN,
	CALL,
	CANY,
	CEXISTS,

	// set
	CSET,
	CSET_ORDERED,
	CSET_ASSIGN,
	CSET_PTR,
	CSET_SORT,
	CSET_ADD,
	CSET_GET,
	CSET_RESULT,
	CSET_AGG,
	CSELF,

	// union
	CUNION,
	CUNION_SET_AGGS,
	CUNION_RECV,

	// table cursor
	CTABLE_OPEN,
	CTABLE_OPEN_LOOKUP,
	CTABLE_PREPARE,
	CTABLE_CLOSE,
	CTABLE_NEXT,
	CTABLE_READB,
	CTABLE_READI8,
	CTABLE_READI16,
	CTABLE_READI32,
	CTABLE_READI64,
	CTABLE_READF,
	CTABLE_READD,
	CTABLE_READT,
	CTABLE_READL,
	CTABLE_READS,
	CTABLE_READJ,
	CTABLE_READV,
	CTABLE_READU,

	// store cursor
	CSTORE_OPEN,
	CSTORE_CLOSE,
	CSTORE_NEXT,
	CSTORE_READ,

	// json cursor
	CJSON_OPEN,
	CJSON_CLOSE,
	CJSON_NEXT,
	CJSON_READ,

	// aggs
	CAGG,
	CCOUNT,
	CAVGI,
	CAVGD,

	// functions
	CCALL,

	// dml
	CINSERT,
	CUPSERT,
	CDELETE,
	CUPDATE,
	CUPDATE_STORE,

	// result
	CSEND,
	CSEND_LOOKUP,
	CSEND_ALL,
	CSEND_FIRST,
	CRECV,
	CRECV_TO,

	// result
	CRESULT,
	CCONTENT,
	CCTE_SET,
	CCTE_GET
};

struct OpDesc
{
	int   id;
	char* name;
};

extern OpDesc ops[];

void op_dump(Code*, CodeData*, Buf*);
