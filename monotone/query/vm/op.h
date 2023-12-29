#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct OpDesc OpDesc;

enum
{
	// control
	CRET,
	CNOP,
	CJMP,
	CJMP_POP,
	CJTR,
	CJNTR,
	CJNTR_POP,

	// result
	CSEND,
	CSEND_SET,
	CRECV,
	CREADY,
	CABORT,

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
	CSTRING_MIN,

	// argument
	CARG,

	// expr
	CBOR,
	CBAND,
	CBXOR,
	CSHL,
	CSHR,

	CNOT,
	CEQU,
	CNEQU,
	CGTE,
	CGT,
	CLTE,
	CLT,

	CADD,
	CSUB,
	CMUL,
	CDIV,
	CMOD,
	CNEG,
	CCAT,
	CIDX,

	// object
	CMAP,
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
	CGROUP_ADD_AGGR,
	CGROUP_ADD,
	CGROUP_GET,
	CGROUP_GET_AGGR,
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

void op_dump(Code*, CodeData*, Buf*, Str*);
void op_relocate(Code*, Code*);
