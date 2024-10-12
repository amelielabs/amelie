#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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

	// result
	CSEND_HASH,
	CSEND,
	CSEND_FIRST,
	CSEND_ALL,
	CRECV,
	CRECV_TO,
	CRESULT,
	CBODY,

	// cte
	CCTE_SET,
	CCTE_GET,

	// misc
	CSLEEP,

	// stack
	CPUSH,
	CPOP,

	// data
	CNULL,
	CBOOL,
	CINT,
	CINT_MIN,
	CREAL,
	CSTRING,
	CINTERVAL,
	CTIMESTAMP,
	CSTRING_MIN,
	CTIMESTAMP_MIN,
	CSWAP,

	// argument
	CARG,

	// expr
	CBOR,
	CBAND,
	CBXOR,
	CSHL,
	CSHR,
	CBINV,

	CNOT,
	CEQU,
	CNEQU,
	CGTE,
	CGT,
	CLTE,
	CLT,
	CIN,
	CLIKE,

	CALL,
	CANY,
	CEXISTS,

	CADD,
	CSUB,
	CMUL,
	CDIV,
	CMOD,
	CNEG,
	CCAT,
	CIDX,

	// object
	COBJ,
	CARRAY,

	// column
	CASSIGN,

	// set
	CSET,
	CSET_ORDERED,
	CSET_SORT,
	CSET_ADD,

	// merge
	CMERGE,
	CMERGE_RECV,

	// group
	CGROUP,
	CGROUP_ADD,
	CGROUP_WRITE,
	CGROUP_GET,
	CGROUP_READ,
	CGROUP_READ_AGGR,
	CGROUP_MERGE_RECV,

	// ref
	CREF,
	CREF_KEY,

	// counters
	CCNTR_INIT,
	CCNTR_GTE,
	CCNTR_LTE,

	// cursor
	CCURSOR_OPEN,
	CCURSOR_OPEN_EXPR,
	CCURSOR_OPEN_CTE,
	CCURSOR_PREPARE,
	CCURSOR_CLOSE,
	CCURSOR_NEXT,
	CCURSOR_READ,
	CCURSOR_IDX,

	// functions
	CCALL,

	// dml
	CINSERT,
	CUPDATE,
	CDELETE,
	CUPSERT
};

struct OpDesc
{
	int   id;
	char* name;
};

extern OpDesc ops[];

void op_dump(Code*, CodeData*, Buf*);
