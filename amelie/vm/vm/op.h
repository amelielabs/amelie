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
	CSWAP,

	// stack
	CPUSH,
	CPOP,

	// consts
	CNULL,
	CBOOL,
	CINT,
	CINT_MIN,
	CDOUBLE,
	CSTRING,
	CJSON,
	CJSON_OBJ,
	CJSON_ARRAY,
	CINTERVAL,
	CTIMESTAMP,
	CSTRING_MIN,
	CTIMESTAMP_MIN,

	// argument
	CARG,

	// null operations
	CNULLOP,
	CIS,

	// logical (any)
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

	// gte
	CGTEII,
	CGTEID,
	CGTEDI,
	CGTEDD,
	CGTELL,
	CGTESS,
	CGTEVV,

	// gt
	CGTII,
	CGTID,
	CGTDI,
	CGTDD,
	CGTLL,
	CGTSS,
	CGTVV,

	// lte
	CLTEII,
	CLTEID,
	CLTEDI,
	CLTEDD,
	CLTELL,
	CLTESS,
	CLTEVV,

	// lt
	CLTII,
	CLTID,
	CLTDI,
	CLTDD,
	CLTLL,
	CLTSS,
	CLTVV,

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
	CSET_SORT,
	CSET_ADD,
	CSET_GET,
	CSET_AGG,

	// merge
	CMERGE,
	CMERGE_RECV,
	CMERGE_RECV_AGG,

	// counters
	CCNTR_INIT,
	CCNTR_GTE,
	CCNTR_LTE,

	// table cursor
	CCURSOR_OPEN,
	CCURSOR_PREPARE,
	CCURSOR_CLOSE,
	CCURSOR_NEXT,
	CCURSOR_READB,
	CCURSOR_READI8,
	CCURSOR_READI16,
	CCURSOR_READI32,
	CCURSOR_READI64,
	CCURSOR_READF,
	CCURSOR_READD,
	CCURSOR_READT,
	CCURSOR_READL,
	CCURSOR_READS,
	CCURSOR_READJ,
	CCURSOR_READV,

	// set cursor
	CCURSOR_SET_OPEN,
	CCURSOR_SET_CLOSE,
	CCURSOR_SET_NEXT,
	CCURSOR_SET_READ,

	// merge cursor
	CCURSOR_MERGE_OPEN,
	CCURSOR_MERGE_CLOSE,
	CCURSOR_MERGE_NEXT,
	CCURSOR_MERGE_READ,

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
	CUPDATE,
	CDELETE,

	// result
	CSEND,
	CSEND_HASH,
	CSEND_GENERATED,
	CSEND_FIRST,
	CSEND_ALL,
	CRECV,
	CRECV_TO,

	// result
	CRESULT,
	CBODY,
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
