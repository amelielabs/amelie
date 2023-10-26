#pragma once

//
// monotone
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
	CJTR,
	CJNTR,

	CSEND,
	CRECV,
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
	CPARTIAL,

	// arguments
	CARG,
	CARG_NAME,

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

	// object ops
	CCOL_SET,
	CCOL_UNSET,

	// set
	CSET,
	CSET_ORDERED,
	CSET_SORT,
	CSET_ADD,
	CSET_SEND,

	// group
	CGROUP,
	CGROUP_ADD_AGGR,
	CGROUP_ADD,
	CGROUP_GET,
	CGROUP_GET_AGGR,

	// counters
	CCNTR_INIT,
	CCNTR_GTE,
	CCNTR_LTE,

	// cursor
	CCURSOR_OPEN,
	CCURSOR_OPEN_EXPR,
	CCURSOR_CLOSE,
	CCURSOR_NEXT,
	CCURSOR_READ,
	CCURSOR_IDX,

	// functions
	CCALL,

	// dml
	CINSERT,
	CUPDATE,
	CDELETE
};

struct OpDesc
{
	int   id;
	char* name;
};

extern OpDesc ops[];

void op_dump(Code*, CodeData*, Buf*, Str*);
