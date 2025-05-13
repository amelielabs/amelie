
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>

void
vm_init(Vm*          self,
        Db*          db,
        Core*        core,
        Executor*    executor,
        Dtr*         dtr,
        FunctionMgr* function_mgr)
{
	self->regs         = NULL;
	self->code         = NULL;
	self->code_data    = NULL;
	self->code_arg     = NULL;
	self->args         = NULL;
	self->core         = core;
	self->executor     = executor;
	self->dtr          = dtr;
	self->program      = NULL;
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
		call_mgr_reset(&self->call_mgr);
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
       Program*  program,
       Code*     code,
       CodeData* code_data,
       Buf*      code_arg,
       Reg*      regs,
       Value*    args,
       Value*    result,
       Content*  content,
       int       start)
{
	self->local     = local;
	self->tr        = tr;
	self->program   = program;
	self->code      = code;
	self->code_data = code_data;
	self->code_arg  = code_arg;
	self->regs      = regs;
	self->args      = args;
	self->result    = result;
	self->content   = content;
	call_mgr_prepare(&self->call_mgr, self->db, local, code_data);

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
		&&cdup,
		&&cmov,

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
		&&cdate,
		&&cvector,
		&&cuuid,

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
		&&cequif,
		&&cequfi,
		&&cequff,
		&&cequll,
		&&cequss,
		&&cequjj,
		&&cequvv,
		&&cequuu,

		// gte
		&&cgteii,
		&&cgteif,
		&&cgtefi,
		&&cgteff,
		&&cgtell,
		&&cgtess,
		&&cgtevv,
		&&cgteuu,

		// gt
		&&cgtii,
		&&cgtif,
		&&cgtfi,
		&&cgtff,
		&&cgtll,
		&&cgtss,
		&&cgtvv,
		&&cgtuu,

		// lte
		&&clteii,
		&&clteif,
		&&cltefi,
		&&clteff,
		&&cltell,
		&&cltess,
		&&cltevv,
		&&clteuu,

		// lt
		&&cltii,
		&&cltif,
		&&cltfi,
		&&cltff,
		&&cltll,
		&&cltss,
		&&cltvv,
		&&cltuu,

		// add
		&&caddii,
		&&caddif,
		&&caddfi,
		&&caddff,
		&&caddtl,
		&&caddll,
		&&caddlt,
		&&cadddi,
		&&caddid,
		&&cadddl,
		&&caddld,
		&&caddvv,

		// sub
		&&csubii,
		&&csubif,
		&&csubfi,
		&&csubff,
		&&csubtl,
		&&csubtt,
		&&csubll,
		&&csubdi,
		&&csubdl,
		&&csubvv,

		// mul
		&&cmulii,
		&&cmulif,
		&&cmulfi,
		&&cmulff,
		&&cmulvv,

		// div
		&&cdivii,
		&&cdivif,
		&&cdivfi,
		&&cdivff,

		// mod
		&&cmodii,

		// neg
		&&cnegi,
		&&cnegf,
		&&cnegl,

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
		&&cset_assign,
		&&cset_ptr,
		&&cset_sort,
		&&cset_add,
		&&cset_get,
		&&cset_result,
		&&cset_agg,
		&&cself,

		// union
		&&cunion,
		&&cunion_set_aggs,
		&&cunion_recv,

		// table cursor
		&&ctable_open,
		&&ctable_open_lookup,
		&&ctable_open_part,
		&&ctable_open_part_lookup,
		&&ctable_open_heap,
		&&ctable_prepare,
		&&ctable_close,
		&&ctable_next,
		&&ctable_readb,
		&&ctable_readi8,
		&&ctable_readi16,
		&&ctable_readi32,
		&&ctable_readi64,
		&&ctable_readf32,
		&&ctable_readf64,
		&&ctable_readt,
		&&ctable_readl,
		&&ctable_readd,
		&&ctable_reads,
		&&ctable_readj,
		&&ctable_readv,
		&&ctable_readu,

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
		&&cavgf,

		// function call
		&&ccall,

		// dml
		&&cinsert,
		&&cupsert,
		&&cdelete,
		&&cupdate,
		&&cupdate_store,

		// executor
		&&csend_shard,
		&&csend_lookup,
		&&csend_lookup_by,
		&&csend_all,
		&&crecv,

		// result
		&&cassign,
		&&cresult,
		&&ccontent,
		&&cref
	};

	int64_t   rc;
	double    dbl;
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

	auto stack       = &self->stack;
	auto cursor_mgr  = &self->cursor_mgr;
	Cursor* cursor;

	register auto r  = self->r.r;
	register auto op = code_at(self->code, start);
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

cdup:
	// [result, value]
	value_copy(&r[op->a], &r[op->b]);
	op_next;

cmov:
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

cdate:
	value_set_date(&r[op->a], op->b);
	op_next;

cvector:
	value_set_vector(&r[op->a], (Vector*)code_data_at(code_data, op->b), NULL);
	op_next;

cuuid:
	value_set_uuid(&r[op->a], (Uuid*)code_data_at(code_data, op->b));
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

cequif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer == r[op->c].dbl);
	op_next;

cequfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl == r[op->c].integer);
	op_next;

cequff:
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

cequuu:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], !uuid_compare(&r[op->b].uuid, &r[op->c].uuid));
	op_next;

// gte
cgteii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].integer);
	op_next;

cgteif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].dbl);
	op_next;

cgtefi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl >= r[op->c].integer);
	op_next;

cgteff:
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

cgteuu:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) >= 0);
	op_next;

// gt
cgtii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].integer);
	op_next;

cgtif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].dbl);
	op_next;

cgtfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl > r[op->c].integer);
	op_next;

cgtff:
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

cgtuu:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) > 0);
	op_next;

// lte
clteii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].integer);
	op_next;

clteif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].dbl);
	op_next;

cltefi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl <= r[op->c].integer);
	op_next;

clteff:
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

clteuu:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) <= 0);
	op_next;

// lt
cltii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].integer);
	op_next;

cltif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].dbl);
	op_next;

cltfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl < r[op->c].integer);
	op_next;

cltff:
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

cltuu:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) < 0);
	op_next;

// add
caddii:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(int64_add_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int + int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

caddif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int + double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double + int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddff:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double + double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddtl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, r[op->b].integer);
		timestamp_add(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
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
		timestamp_set_unixtime(&ts, r[op->c].integer);
		timestamp_add(&ts, &r[op->b].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

cadddi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_add(r[op->b].integer, r[op->c].integer));
	op_next;

caddid:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_add(r[op->c].integer, r[op->b].integer));
	op_next;

cadddl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		// convert julian to timestamp add interval and convert to unixtime
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->b].integer);
		timestamp_add(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

caddld:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->c].integer);
		timestamp_add(&ts, &r[op->b].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
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
	{
		if (unlikely(int64_sub_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int - int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

csubif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int - double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double - int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubff:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double - double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubtl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, r[op->b].integer);
		timestamp_sub(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

csubtt:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_init(&iv);
		iv.us = r[op->b].integer - r[op->c].integer;
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

csubll:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_sub(&iv, &r[op->b].interval, &r[op->c].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

csubdi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_sub(r[op->b].integer, r[op->c].integer));
	op_next;

csubdl:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		// convert julian to timestamp sub interval and convert to unixtime
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->b].integer);
		timestamp_sub(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
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
	{
		if (unlikely(int64_mul_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int * int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

cmulif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int * double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cmulfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double * int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cmulff:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double * double overflow");
		value_set_double(&r[op->a], dbl);
	}
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

cdivif:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].dbl == 0))
			error("zero division");
		if (unlikely(double_div_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int / double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cdivfi:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		if (unlikely(double_div_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double / int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cdivff:
	if (likely(value_is(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].dbl == 0))
			error("zero division");
		if (unlikely(double_div_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double / double overflow");
		value_set_double(&r[op->a], dbl);
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

cnegf:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
		value_set_double(&r[op->a], -r[op->b].dbl);
	op_next;

cnegl:
	if (likely(value_is_unary(&r[op->a], &r[op->b])))
	{
		interval_neg(&iv, &r[op->b].interval);
		value_set_interval(&r[op->a], &iv);
	}
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

cset_assign:
	// [set, child_set]
	set = (Set*)r[op->a].store;
	set_assign(set, (Set*)r[op->b].store);
	value_reset(&r[op->b]);
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

cunion:
	// [union, set, limit, offset]
	cunion(self, op);
	op_next;

cunion_set_aggs:
	// [union, aggs]
	union_set_aggs((Union*)r[op->a].store, (int*)code_data_at(code_data, op->b));
	op_next;

cunion_recv:
	// [union, stmt, limit, offset]
	cunion_recv(self, op);
	op_next;

// table cursor
ctable_open:
	op = ctable_open(self, op, false, false);
	op_jmp;

ctable_open_lookup:
	op = ctable_open(self, op, true, false);
	op_jmp;

ctable_open_part:
	op = ctable_open(self, op, false, true);
	op_jmp;

ctable_open_part_lookup:
	op = ctable_open(self, op, true, true);
	op_jmp;

ctable_open_heap:
	op = ctable_open_heap(self, op);
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

ctable_readf32:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_double(&r[op->a], *(float*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readf64:
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

ctable_readd:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_date(&r[op->a], *(int32_t*)ptr);
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

ctable_readu:
	ptr = row_at(iterator_at(cursor_mgr_of(cursor_mgr, op->b)->it), op->c);
	if (likely(ptr))
		value_set_uuid(&r[op->a], (Uuid*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

// set/merge cursor
cstore_open:
	// [cursor, store, _eof]
	cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->r = op->b;
	cursor->type = CURSOR_STORE;
	if (likely(r[op->b].type == TYPE_STORE))
	{
		// jmp on success or skip to the next op on eof
		cursor->it_store = store_iterator(r[op->b].store);
		if (likely(store_iterator_has(cursor->it_store)))
			op++;
		else
			op = code_at(self->code, op->c);
	} else {
		assert(r[op->b].type == TYPE_NULL);
		op = code_at(self->code, op->c);
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
	// [cursor, json, _eof]
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
			op++;
		} else
		{
			cursor->pos_size = json_sizeof(cursor->pos);
			op = code_at(self->code, op->c);
		}
	} else {
		assert(r[op->b].type == TYPE_NULL);
		op = code_at(self->code, op->c);
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

cavgf:
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
	call.type     = CALL_EXECUTE;
	call.function = (Function*)op->b;
	call.mgr      = &self->call_mgr;
	call.context  = NULL;
	if (op->d != -1)
		call.context = call_mgr_at(&self->call_mgr, op->d);
	call.function->function(&call);
	stack_popn(&self->stack, op->c);
	op_next;

cinsert:
	cinsert(self, op);
	op_next;

cupsert:
	op = cupsert(self, op);
	op_jmp;

cdelete:
	cdelete(self, op);
	op_next;

cupdate:
	cupdate(self, op);
	op_next;

cupdate_store:
	cupdate_store(self, op);
	op_next;

csend_shard:
	csend_shard(self, op);
	op_next;

csend_lookup:
	csend_lookup(self, op);
	op_next;

csend_lookup_by:
	csend_lookup_by(self, op);
	op_next;

csend_all:
	csend_all(self, op);
	op_next;

crecv:
	crecv(self, op);
	op_next;

cassign:
	// [result, store]
	cassign(self, op);
	op_next;

cresult:
	// [result]
	value_move(self->result, &r[op->a]);
	op_next;

ccontent:
	// [r, columns*, format*]
	content_write(self->content, (Str*)op->c, (Columns*)op->b, &r[op->a]);
	op_next;

cref:
	// [result, r]
	value_copy(&r[op->a], reg_at(self->regs, op->b));
	op_next;
}
