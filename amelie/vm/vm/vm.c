
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>

void
vm_init(Vm*          self,
        Db*          db,
        Uuid*        node,
        Executor*    executor,
        Dtr*         dtr,
        FunctionMgr* function_mgr)
{
	self->code         = NULL;
	self->code_data    = NULL;
	self->code_arg     = NULL;
	self->args         = NULL;
	self->node         = node;
	self->executor     = executor;
	self->dtr          = dtr;
	self->cte          = NULL;
	self->result       = NULL;
	self->content      = NULL;
	self->tr           = NULL;
	self->local        = NULL;
	self->function_mgr = function_mgr;
	self->db           = db;
	reg_init(&self->r);
	stack_init(&self->stack);
	cursor_mgr_init(&self->cursor_mgr);
	call_mgr_init(&self->call_mgr);
}

void
vm_free(Vm* self)
{
	vm_reset(self);
	cursor_mgr_free(&self->cursor_mgr);
	call_mgr_free(&self->call_mgr);
	stack_free(&self->stack);
	reg_free(&self->r);
}

void
vm_reset(Vm* self)
{
	if (self->code_data)
		call_mgr_reset(&self->call_mgr, self->code_data, self);
	reg_reset(&self->r);
	stack_reset(&self->stack);
	cursor_mgr_reset(&self->cursor_mgr);
	self->code      = NULL;
	self->code_data = NULL;
	self->code_arg  = NULL;
	self->args      = NULL;
}

#define op_start goto *ops[(op)->op]
#define op_next  goto *ops[(++op)->op]
#define op_jmp   goto *ops[(op)->op]

hot void
vm_run(Vm*       self,
       Local*    local,
       Tr*       tr,
       Code*     code,
       CodeData* code_data,
       Buf*      code_arg,
       Buf*      args,
       Result*   cte,
       Value*    result,
       Content*  content,
       int       start)
{
	self->local     = local;
	self->tr        = tr;
	self->code      = code;
	self->code_data = code_data;
	self->code_arg  = code_arg;
	self->args      = args;
	self->cte       = cte;
	self->result    = result;
	self->content   = content;
	reg_prepare(&self->r);
	call_mgr_prepare(&self->call_mgr, code_data);

	const void* ops[] =
	{
		// control flow
		&&cret,
		&&cnop,
		&&cjmp,
		&&cjtr,
		&&cjntr,
		&&cjgted,
		&&cjltd,

		// values
		&&cfree,
		&&cswap,

		// stack
		&&cpush,
		&&cpop,

		// consts
		&&cnull,
		&&cbool,
		&&cint,
		&&cdouble,
		&&cstring,
		&&cjson,
		&&cjson_obj,
		&&cjson_array,
		&&cinterval,
		&&ctimestamp,
		&&cvector,

		// argument
		&&carg,
		&&cexcluded,

		// null operations
		&&cnullop,
		&&cis,

		// logic
		&&cand,
		&&cor,
		&&cnot,

		// bitwise operations
		&&cborii,
		&&cbandii,
		&&cbxorii,
		&&cshlii,
		&&cshrii,
		&&cbinvi,

		// equ
		&&cequii,
		&&cequid,
		&&cequdi,
		&&cequdd,
		&&cequll,
		&&cequss,
		&&cequjj,
		&&cequvv,

		// gte
		&&cgteii,
		&&cgteid,
		&&cgtedi,
		&&cgtedd,
		&&cgtell,
		&&cgtess,
		&&cgtevv,

		// gt
		&&cgtii,
		&&cgtid,
		&&cgtdi,
		&&cgtdd,
		&&cgtll,
		&&cgtss,
		&&cgtvv,

		// lte
		&&clteii,
		&&clteid,
		&&cltedi,
		&&cltedd,
		&&cltell,
		&&cltess,
		&&cltevv,

		// lt
		&&cltii,
		&&cltid,
		&&cltdi,
		&&cltdd,
		&&cltll,
		&&cltss,
		&&cltvv,

		// add
		&&caddii,
		&&caddid,
		&&cadddi,
		&&cadddd,
		&&caddtl,
		&&caddll,
		&&caddlt,
		&&caddvv,

		// sub
		&&csubii,
		&&csubid,
		&&csubdi,
		&&csubdd,
		&&csubtl,
		&&csubll,
		&&csubvv,

		// mul
		&&cmulii,
		&&cmulid,
		&&cmuldi,
		&&cmuldd,
		&&cmulvv,

		// div
		&&cdivii,
		&&cdivid,
		&&cdivdi,
		&&cdivdd,

		// mod
		&&cmodii,

		// neg
		&&cnegi,
		&&cnegd,

		// cat
		&&ccatss,

		// idx
		&&cidxjs,
		&&cidxji,
		&&cidxvi,

		// dot
		&&cdotjs,

		// like
		&&clikess,

		// set operations
		&&cin,
		&&call,
		&&cany,
		&&cexists,

		// set
		&&cset,
		&&cset_ordered,
		&&cset_ptr,
		&&cset_sort,
		&&cset_add,
		&&cset_get,
		&&cset_result,
		&&cset_agg,
		&&cself,

		// set merge
		&&cmerge,
		&&cmerge_recv,
		&&cmerge_recv_agg,

		// table cursor
		&&ctable_open,
		&&ctable_prepare,
		&&ctable_close,
		&&ctable_next,
		&&ctable_readb,
		&&ctable_readi8,
		&&ctable_readi16,
		&&ctable_readi32,
		&&ctable_readi64,
		&&ctable_readf,
		&&ctable_readd,
		&&ctable_readt,
		&&ctable_readl,
		&&ctable_reads,
		&&ctable_readj,
		&&ctable_readv,

		// store cursor
		&&cstore_open,
		&&cstore_close,
		&&cstore_next,
		&&cstore_read,

		// json cursor
		&&cjson_open,
		&&cjson_close,
		&&cjson_next,
		&&cjson_read,

		// aggs
		&&cagg,
		&&ccount,
		&&cavgi,
		&&cavgd,

		// function call
		&&ccall,

		// dml
		&&cinsert,
		&&cupsert,
		&&cupdate,
		&&cdelete,

		// executor
		&&csend,
		&&csend_hash,
		&&csend_generated,
		&&csend_first,
		&&csend_all,
		&&crecv,
		&&crecv_to,

		// result
		&&cresult,
		&&ccontent,
		&&ccte_set,
		&&ccte_get
	};

	register auto stack = &self->stack;
	register auto r     =  self->r.r;
	register auto op    = code_at(self->code, start);
	auto cursor_mgr     = &self->cursor_mgr;
	Cursor* cursor;

	int64_t   rc;
	Str       string;
	Value*    a;
	Value*    b;
	Value*    c;
	Set*      set;
	Timestamp ts;
	Interval  iv;
	Vector*   vector;
	Buf*      buf;
	uint8_t*  json;
	void*     ptr;
	Call      call;

	str_init(&string);
	op_start;

cret:
	return;

cnop:
	op_next;

cjmp:
	op = code_at(code, op->a);
	op_jmp;

cjtr:
	rc = value_is_true(&r[op->b]);
	value_free(&r[op->b]);
	if (rc)
	{
		op = code_at(code, op->a);
		op_jmp;
	}
	op_next;

cjntr:
	rc = value_is_true(&r[op->b]);
	value_free(&r[op->b]);
	if (! rc)
	{
		op = code_at(code, op->a);
		op_jmp;
	}
	op_next;

cjgted:
	// [value, jmp]
	if (--r[op->a].integer >= 0)
	{
		op = code_at(code, op->b);
		op_jmp;
	}
	op_next;

cjltd:
	// [value, jmp]
	if (--r[op->a].integer < 0)
	{
		op = code_at(code, op->b);
		op_jmp;
	}
	op_next;

cfree:
	// [value]
	value_free(&r[op->a]);
	op_next;

cswap:
	// a = b
	r[op->a] = r[op->b];
	value_reset(&r[op->b]);
	op_next;

cpush:
	*stack_push(stack) = r[op->a];
	value_reset(&r[op->a]);
	op_next;

cpop:
	value_free(stack_pop(stack));
	op_next;

cnull:
	value_set_null(&r[op->a]);
	op_next;

cbool:
	value_set_bool(&r[op->a], op->b);
	op_next;

cint:
	value_set_int(&r[op->a], op->b);
	op_next;

cdouble:
	value_set_double(&r[op->a], code_data_at_double(code_data, op->b));
	op_next;

cstring:
	code_data_at_string(code_data, op->b, &string);
	value_set_string(&r[op->a], &string, NULL);
	op_next;

cjson:
	value_decode(&r[op->a], code_data_at(code_data, op->b), NULL);
	op_next;

cjson_obj:
	value_obj(&r[op->a], local->timezone, stack, op->b);
	stack_popn(stack, op->b);
	op_next;

cjson_array:
	value_array(&r[op->a], local->timezone, stack, op->b);
	stack_popn(stack, op->b);
	op_next;

cinterval:
	value_set_interval(&r[op->a], (Interval*)code_data_at(code_data, op->b));
	op_next;

ctimestamp:
	value_set_timestamp(&r[op->a], op->b);
	op_next;

cvector:
	value_set_vector(&r[op->a], (Vector*)code_data_at(code_data, op->b), NULL);
	op_next;

carg:
	// [result, order]
	// todo: read array
	// value_read_arg(&r[op->a], self->args, op->b);
	op_next;

cexcluded:
	// [result, cursor, order]
	cursor = cursor_mgr_of(cursor_mgr, op->b);
	if (unlikely(! cursor->ref))
		error("unexpected EXCLUDED usage");
	value_copy(&r[op->a], cursor->ref + op->c);
	op_next;

cnullop:
	// [a, b, c]
	value_set_null(&r[op->a]);
	value_free(&r[op->b]);
	value_free(&r[op->c]);
	op_next;

cis:
	// [a, b]
	value_set_bool(&r[op->a], r[op->b].type == TYPE_NULL);
	value_free(&r[op->b]);
	op_next;

cand:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], value_is_true(&r[op->b]) && value_is_true(&r[op->c]));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cor:
	a = &r[op->a];
	b = &r[op->b];
	c = &r[op->c];
	if (unlikely(b->type == TYPE_NULL && c->type == TYPE_NULL))
		value_set_null(a);
	else
		value_set_bool(a, value_is_true(b) || value_is_true(c));
	value_free(b);
	value_free(c);
	op_next;

cnot:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
	{
		value_set_bool(&r[op->a], !value_is_true(&r[op->b]));
		value_free(&r[op->b]);
	}
	op_next;

cborii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer | r[op->c].integer);
	op_next;

cbandii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer & r[op->c].integer);
	op_next;

cbxorii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer ^ r[op->c].integer);
	op_next;

cshlii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer << r[op->c].integer);
	op_next;

cshrii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer >> r[op->c].integer);
	op_next;

cbinvi:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
		value_set_int(&r[op->a], ~r[op->b].integer);
	op_next;

// equ
cequii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer == r[op->c].integer);
	op_next;

cequid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer == r[op->c].dbl);
	op_next;

cequdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl == r[op->c].integer);
	op_next;

cequdd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl == r[op->c].dbl);
	op_next;

cequll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], !interval_compare(&r[op->b].interval, &r[op->c].interval));
	op_next;

cequss:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !str_compare_fn(&r[op->b].string, &r[op->c].string));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cequjj:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !json_compare(r[op->b].json, r[op->c].json));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cequvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !vector_compare(r[op->b].vector, r[op->c].vector));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// gte
cgteii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].integer);
	op_next;

cgteid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].dbl);
	op_next;

cgtedi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl >= r[op->c].integer);
	op_next;

cgtedd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl >= r[op->c].dbl);
	op_next;

cgtell:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) >= 0);
	op_next;

cgtess:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) >= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgtevv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) >= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// gt
cgtii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].integer);
	op_next;

cgtid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].dbl);
	op_next;

cgtdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl > r[op->c].integer);
	op_next;

cgtdd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl > r[op->c].dbl);
	op_next;

cgtll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) > 0);
	op_next;

cgtss:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) > 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgtvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) > 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// lte
clteii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].integer);
	op_next;

clteid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].dbl);
	op_next;

cltedi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl <= r[op->c].integer);
	op_next;

cltedd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl <= r[op->c].dbl);
	op_next;

cltell:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) <= 0);
	op_next;

cltess:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) <= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cltevv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) <= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// lt
cltii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].integer);
	op_next;

cltid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].dbl);
	op_next;

cltdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl < r[op->c].integer);
	op_next;

cltdd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl < r[op->c].dbl);
	op_next;

cltll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) < 0);
	op_next;

cltss:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) < 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cltvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) < 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// add
caddii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer + r[op->c].integer);
	op_next;

caddid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].integer + r[op->c].dbl);
	op_next;

cadddi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl + r[op->c].integer);
	op_next;

cadddd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl + r[op->c].dbl);
	op_next;

caddtl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_read_value(&ts, r[op->b].integer);
		timestamp_add(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_of(&ts, NULL));
	}
	op_next;

caddll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_add(&iv, &r[op->b].interval, &r[op->c].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

caddlt:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_read_value(&ts, r[op->c].integer);
		timestamp_add(&ts, &r[op->b].interval);
		value_set_timestamp(&r[op->a], timestamp_of(&ts, NULL));
	}
	op_next;

caddvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_claim(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_add(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// sub
csubii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer - r[op->c].integer);
	op_next;

csubid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].integer - r[op->c].dbl);
	op_next;

csubdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl - r[op->c].integer);
	op_next;

csubdd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl - r[op->c].dbl);
	op_next;

csubtl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_read_value(&ts, r[op->b].integer);
		timestamp_sub(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_of(&ts, NULL));
	}
	op_next;

csubll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_sub(&iv, &r[op->b].interval, &r[op->c].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

csubvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_claim(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_sub(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// mul
cmulii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer * r[op->c].integer);
	op_next;

cmulid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].integer * r[op->c].dbl);
	op_next;

cmuldi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl * r[op->c].integer);
	op_next;

cmuldd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_double(&r[op->a], r[op->b].dbl * r[op->c].dbl);
	op_next;

cmulvv:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_claim(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_mul(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// div
cdivii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		value_set_int(&r[op->a], r[op->b].integer / r[op->c].integer);
	}
	op_next;

cdivid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].dbl == 0))
			error("zero division");
		value_set_double(&r[op->a], r[op->b].integer / r[op->c].dbl);
	}
	op_next;

cdivdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		value_set_double(&r[op->a], r[op->b].dbl / r[op->c].integer);
	}
	op_next;

cdivdd:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].dbl == 0))
			error("zero division");
		value_set_double(&r[op->a], r[op->b].dbl / r[op->c].dbl);
	}
	op_next;

// mod
cmodii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		value_set_int(&r[op->a], r[op->b].integer % r[op->c].integer);
	}
	op_next;

// neg
cnegi:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
		value_set_int(&r[op->a], -r[op->b].integer);
	op_next;

cnegd:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
		value_set_double(&r[op->a], -r[op->b].dbl);
	op_next;

// cat
ccatss:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		buf = buf_create();
		buf_write_str(buf, &r[op->b].string);
		buf_write_str(buf, &r[op->c].string);
		buf_str(buf, &string);
		value_set_string(&r[op->a], &string, buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cidxjs:
	// [result, object, pos]
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		// {}['path']
		a = &r[op->a];
		b = &r[op->b];
		c = &r[op->c];
		json = b->json;
		if (likely(json_is_obj(json)))
		{
			if (! json_obj_find(&json, str_of(&c->string), str_size(&c->string))) {
				value_set_null(a);
			} else {
				value_set_json(a, json, json_sizeof(json), b->buf);
				if (b->buf)
					buf_ref(b->buf);
			}
		} else
		if (json_is_null(json)) {
			value_set_null(a);
		} else {
			error("[]: object expected");
		}
		value_free(b);
		value_free(c);
	}
	op_next;

cidxji:
	// [result, array, pos]
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		// [][pos]
		a = &r[op->a];
		b = &r[op->b];
		c = &r[op->c];
		json = b->json;
		if (likely(json_is_array(json)))
		{
			if (! json_array_find(&json, c->integer)) {
				value_set_null(a);
			} else
			{
				value_set_json(a, json, json_sizeof(json), b->buf);
				if (b->buf)
					buf_ref(b->buf);
			}
		} else
		if (json_is_null(json)) {
			value_set_null(a);
		} else {
			error("[]: array expected");
		}
		value_free(b);
		value_free(c);
	}
	op_next;

cidxvi:
	// [][pos]
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		a = &r[op->a];
		b = &r[op->b];
		c = &r[op->c];
		if (unlikely(c->integer < 0 || c->integer >= b->vector->size))
			error("[]: vector index is out of bounds");
		value_set_double(a, b->vector->value[c->integer]);
		value_free(b);
		value_free(c);
	}
	op_next;

cdotjs:
	// [result, object, pos]
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		// {}.path
		a = &r[op->a];
		b = &r[op->b];
		c = &r[op->c];
		json = b->json;
		if (likely(json_is_obj(json)))
		{
			if (! json_obj_find_path(&json, &c->string)) {
				value_set_null(a);
			} else {
				value_set_json(a, json, json_sizeof(json), b->buf);
				if (b->buf)
					buf_ref(b->buf);
			}
		} else
		if (json_is_null(json)) {
			value_set_null(a);
		} else {
			error(".: object expected");
		}
		value_free(b);
		value_free(c);
	}
	op_next;

clikess:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_like(&r[op->a], &r[op->b], &r[op->c]);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cin:
	// [result, value, count]
	value_in(&r[op->a], &r[op->b], stack_at(stack, op->c), op->c);
	stack_popn(stack, op->c);
	op_next;

call:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_all(&r[op->a], &r[op->b], &r[op->c], op->d);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cany:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_any(&r[op->a], &r[op->b], &r[op->c], op->d);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cexists:
	// [result, set]
	value_exists(&r[op->a], &r[op->b]);
	value_free(&r[op->b]);
	op_next;

cset:
	// [set, cols, keys]
	set = set_create();
	set_prepare(set, op->b, op->c, NULL);
	value_set_store(&r[op->a], &set->store);
	op_next;

cset_ordered:
	// [set, cols, keys, order]
	set = set_create();
	set_prepare(set, op->b, op->c, (bool*)code_data_at(code_data, op->d));
	value_set_store(&r[op->a], &set->store);
	op_next;

cset_ptr:
	// [set, set*]
	set = (Set*)op->b;
	value_set_store(&r[op->a], &set->store);
	store_ref(&set->store);
	op_next;

cset_sort:
	set_sort((Set*)r[op->a].store);
	op_next;

cset_add:
	// [set]
	set = (Set*)r[op->a].store;
	set_add(set, stack_at(stack, set->count_columns_row));
	stack_popn(stack, set->count_columns_row);
	op_next;

cset_get:
	// [result, set]
	// get existing or create new row by key,
	// return the row reference
	set = (Set*)r[op->b].store;
	rc = set_get(set, stack_at(stack, set->count_keys), true);
	value_set_int(&r[op->a], rc);
	stack_popn(stack, set->count_keys);
	op_next;

cset_result:
	// [result, set]
	set = (Set*)r[op->b].store;
	// return first row column and free set
	if (! set->count_rows)
		value_set_null(&r[op->a]);
	else
		value_move(&r[op->a], set_column(set, 0, 0));
	value_free(&r[op->b]);
	op_next;

cset_agg:
	// [set, row, aggs]
	set = (Set*)r[op->a].store;
	agg_write(set, stack_at(stack, set->count_columns), r[op->b].integer,
	          (int*)code_data_at(code_data, op->c));
	stack_popn(stack, set->count_columns);
	op_next;

cself:
	// [result, set, row, seed]
	set = (Set*)r[op->b].store;
	// return aggregate state or seed, if the state is null
	a = stack_pop(stack);
	b = set_column(set, r[op->c].integer, a->integer);
	if (likely(b->type != TYPE_NULL))
		value_copy(&r[op->a], b);
	else
		value_copy(&r[op->a], &r[op->d]);
	op_next;

cmerge:
	// [merge, set, limit, offset]
	cmerge(self, op);
	op_next;

cmerge_recv:
	// [merge, stmt, limit, offset]
	cmerge_recv(self, op);
	op_next;

cmerge_recv_agg:
	// [set, stmt]
	cmerge_recv_agg(self, op);
	op_next;

// table cursor
ctable_open:
	op = ctable_open(self, op);
	op_jmp;

ctable_prepare:
	ctable_prepare(self, op);
	op_next;

ctable_close:
	cursor_reset(cursor_mgr_of(cursor_mgr, op->a));
	op_next;

ctable_next:
	// [target_id, _on_success]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	iterator_next(cursor->it);
	// jmp on success or skip to the next op on eof
	op = likely(iterator_has(cursor->it)) ? code_at(self->code, op->b) : op + 1;
	op_jmp;

ctable_readb:
	// [result, cursor, column]
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_bool(&r[op->a], *(int8_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi8:
	// [result, cursor, column]
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int8_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi16:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int16_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi32:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int32_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi64:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int64_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readf:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_double(&r[op->a], *(float*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readd:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_double(&r[op->a], *(double*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readt:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_timestamp(&r[op->a], *(int64_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readl:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_interval(&r[op->a], (Interval*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_reads:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
	{
		json_read_string((uint8_t**)&ptr, &r[op->a].string);
		r[op->a].type = TYPE_STRING;
		r[op->a].buf  = NULL;
	} else {
		value_set_null(&r[op->a]);
	}
	op_next;

ctable_readj:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_json(&r[op->a], ptr, json_sizeof(ptr), NULL);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readv:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_vector(&r[op->a], (Vector*)ptr, NULL);
	else
		value_set_null(&r[op->a]);
	op_next;

// set/merge cursor
cstore_open:
	// [cursor, store, _on_success]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->r = op->b;
	cursor->type = CURSOR_STORE;
	if (likely(r[op->b].type == TYPE_STORE))
	{
		// jmp on success or skip to the next op on eof
		cursor->it_store = store_iterator(r[op->b].store);
		if (likely(store_iterator_has(cursor->it_store)))
			op = code_at(self->code, op->c);
		else
			op++;
	} else {
		assert(r[op->b].type == TYPE_NULL);
		op++;
	}
	op_jmp;

cstore_close:
	// [cursor]
	cursor = cursor_mgr_of(cursor_mgr, op->a);
	value_free(&r[cursor->r]);
	cursor_reset(cursor);
	op_next;

cstore_next:
	// [target_id, _on_success]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	store_iterator_next(cursor->it_store);
	// jmp on success or skip to the next op on eof
	if (likely(store_iterator_has(cursor->it_store)))
		op = code_at(self->code, op->b);
	else
		op++;
	op_jmp;

cstore_read:
	// [result, cursor, column]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->b);
	value_copy(&r[op->a], &store_iterator_at(cursor->it_store)[op->c]);
	op_next;

cjson_open:
	// [cursor, json, _on_success]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->r = op->b;
	cursor->type = CURSOR_JSON;
	if (likely(r[op->b].type == TYPE_JSON))
	{
		if (unlikely(! json_is_array(r[op->b].json)))
			error("FROM: json array expected");
		// jmp on success or skip to the next op on eof
		cursor->pos = r[op->b].json;
		json_read_array(&cursor->pos);
		if (likely(! json_is_array_end(cursor->pos))) {
			cursor->pos_size = json_sizeof(cursor->pos);
			op = code_at(self->code, op->c);
		} else {
			op++;
		}
	} else {
		assert(r[op->b].type == TYPE_NULL);
		op++;
	}
	op_jmp;

cjson_close:
	// [cursor]
	cursor = cursor_mgr_of(cursor_mgr, op->a);
	value_free(&r[cursor->r]);
	cursor_reset(cursor);
	op_next;

cjson_next:
	// [target_id, _on_success]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	json_skip(&cursor->pos);
	if (likely(! json_read_array_end(&cursor->pos))) {
		cursor->pos_size = json_sizeof(cursor->pos);
		op = code_at(self->code, op->b);
	} else {
		op++;
	}
	op_jmp;

cjson_read:
	// [result, cursor]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->b);
	value_set_json(&r[op->a], cursor->pos, cursor->pos_size, NULL);
	op_next;

cagg:
	// [result, cursor, column]
	r[op->a] = store_iterator_at(cursor_mgr_of(&self->cursor_mgr, op->b)->it_store)[op->c];
	op_next;

ccount:
	// [result, cursor, column]
	c = &store_iterator_at(cursor_mgr_of(&self->cursor_mgr, op->b)->it_store)[op->c];
	if (likely(c->type == TYPE_INT))
		value_set_int(&r[op->a], c->integer);
	else
		value_set_int(&r[op->a], 0);
	op_next;

cavgi:
	// [result, cursor, column]
	c = &store_iterator_at(cursor_mgr_of(&self->cursor_mgr, op->b)->it_store)[op->c];
	if (likely(c->type == TYPE_AVG))
		value_set_int(&r[op->a], avg_int(&c->avg));
	else
		value_set_null(&r[op->a]);
	op_next;

cavgd:
	// [result, cursor, column]
	c = &store_iterator_at(cursor_mgr_of(&self->cursor_mgr, op->b)->it_store)[op->c];
	if (likely(c->type == TYPE_AVG))
		value_set_double(&r[op->a], avg_double(&c->avg));
	else
		value_set_null(&r[op->a]);
	op_next;

ccall:
	// [result, function, argc, call_id]
	call.argc     = op->c;
	call.argv     = stack_at(stack, op->c);
	call.result   = &r[op->a];
	call.vm       = self;
	call.type     = CALL_EXECUTE;
	call.function = (Function*)op->b;
	call.context  = NULL;
	if (op->d != -1)
		call.context = call_mgr_at(&self->call_mgr, op->d);
	call.function->main(&call);
	stack_popn(&self->stack, op->c);
	op_next;

cinsert:
	cinsert(self, op);
	op_next;

cupsert:
	op = cupsert(self, op);
	op_jmp;

cupdate:
	cupdate(self, op);
	op_next;

cdelete:
	cdelete(self, op);
	op_next;

csend:
	csend(self, op);
	op_next;

csend_hash:
	csend_hash(self, op);
	op_next;

csend_generated:
	csend_generated(self, op);
	op_next;

csend_first:
	csend_first(self, op);
	op_next;

csend_all:
	csend_all(self, op);
	op_next;

crecv:
	crecv(self, op);
	op_next;

crecv_to:
	crecv_to(self, op);
	op_next;

cresult:
	// [result]
	value_move(self->result, &r[op->a]);
	op_next;

ccontent:
	// [cte, columns*, format*]
	content_write(self->content, (Str*)op->c, (Columns*)op->b,
	              result_at(cte, op->a));
	op_next;

ccte_set:
	// [cte_order, result]
	value_move(result_at(cte, op->a), &r[op->b]);
	op_next;

ccte_get:
	// [result, cte_order]
	value_copy(&r[op->a], result_at(cte, op->b));
	op_next;
}
