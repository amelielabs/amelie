
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>

void
vm_init(Vm* self, Core* core, Dtr* dtr)
{
	self->code        = NULL;
	self->code_data   = NULL;
	self->code_arg    = NULL;
	self->upsert      = NULL;
	self->allow_close = true;
	self->refs        = NULL;
	self->args        = NULL;
	self->core        = core;
	self->dtr         = dtr;
	self->program     = NULL;
	self->tr          = NULL;
	self->local       = NULL;
	reg_init(&self->r);
	stack_init(&self->stack);
	fn_mgr_init(&self->fn_mgr);
}

void
vm_free(Vm* self)
{
	vm_reset(self);
	fn_mgr_free(&self->fn_mgr);
	stack_free(&self->stack);
	reg_free(&self->r);
}

void
vm_reset(Vm* self)
{
	if (self->code_data)
		fn_mgr_reset(&self->fn_mgr);
	reg_reset(&self->r);
	stack_reset(&self->stack);
	self->code        = NULL;
	self->code_data   = NULL;
	self->code_arg    = NULL;
	self->refs        = NULL;
	self->args        = NULL;
	self->upsert      = NULL;
	self->allow_close = true;
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
       Value*    refs,
       Value*    args,
       Return*   ret,
       bool      allow_close,
       int       start)
{
	self->local       = local;
	self->tr          = tr;
	self->program     = program;
	self->code        = code;
	self->code_data   = code_data;
	self->code_arg    = code_arg;
	self->refs        = refs;
	self->args        = args;
	self->allow_close = allow_close;
	fn_mgr_prepare(&self->fn_mgr, local, code_data);

	const void* ops[] =
	{
		// control flow
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
		&&cpush_ref,
		&&cpush_var,
		&&cpush_nulls,
		&&cpush_bool,
		&&cpush_int,
		&&cpush_double,
		&&cpush_string,
		&&cpush_json,
		&&cpush_interval,
		&&cpush_timestamp,
		&&cpush_date,
		&&cpush_vector,
		&&cpush_uuid,
		&&cpush_value,
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
		&&cvalue,

		// upsert
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
		&&cset_distinct,
		&&cset_ptr,
		&&cset_sort,
		&&cset_add,
		&&cset_get,
		&&cset_agg,
		&&cset_agg_merge,
		&&cself,

		// union
		&&cunion,
		&&cunion_limit,
		&&cunion_offset,
		&&cunion_add,
		&&crecv_aggs,
		&&crecv,

		// table cursor
		&&ctable_open,
		&&ctable_open_lookup,
		&&ctable_open_part,
		&&ctable_open_part_lookup,
		&&ctable_prepare,
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
		&&cstore_next,
		&&cstore_read,

		// json cursor
		&&cjson_open,
		&&cjson_next,
		&&cjson_read,

		// aggs
		&&cagg,
		&&ccount,
		&&cavgi,
		&&cavgf,

		// dml
		&&cinsert,
		&&cupsert,
		&&cdelete,
		&&cupdate,
		&&cupdate_store,

		// system
		&&ccheckpoint,

		// user
		&&cuser_create_token,
		&&cuser_create,
		&&cuser_drop,
		&&cuser_alter,

		// replica
		&&creplica_create,
		&&creplica_drop,

		// replication
		&&crepl_start,
		&&crepl_stop,
		&&crepl_subscribe,
		&&crepl_unsubscribe,

		// ddl
		&&cddl,

		// executor
		&&csend_shard,
		&&csend_lookup,
		&&csend_lookup_by,
		&&csend_all,
		&&cclose,

		// var
		&&cvar,
		&&cvar_mov,
		&&cvar_set,
		&&cfirst,
		&&cref,

		// call / return
		&&ccall,
		&&ccall_udf,
		&&cret
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
	Fn        fn;
	str_init(&string);

	auto stack = &self->stack;
	auto r     = self->r.r;
	auto op    = code_at(self->code, start);
	op_start;

cnop:
	op_next;

cjmp:
	// [jmp]
	op = code_at(code, op->a);
	op_jmp;

cjtr:
	// [jmp, value]
	rc = value_is_true(&r[op->b]);
	value_free(&r[op->b]);
	if (rc)
	{
		op = code_at(code, op->a);
		op_jmp;
	}
	op_next;

cjntr:
	// [jmp, value]
	rc = value_is_true(&r[op->b]);
	value_free(&r[op->b]);
	if (! rc)
	{
		op = code_at(code, op->a);
		op_jmp;
	}
	op_next;

cjgted:
	// [jmp value]
	if (--r[op->b].integer >= 0)
	{
		op = code_at(code, op->a);
		op_jmp;
	}
	op_next;

cjltd:
	// [jmp, value]
	if (--r[op->b].integer < 0)
	{
		op = code_at(code, op->a);
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
	value_move(&r[op->a], &r[op->b]);
	op_next;

cpush:
	// [value]
	*stack_push(stack) = r[op->a];
	value_reset(&r[op->a]);
	op_next;

cpush_ref:
	// [value, dup, not_null]
	if (unlikely(op->c && r[op->a].type == TYPE_NULL))
		error("argument cannot be NULL");
	if (op->b)
	{
		a = stack_push(stack);
		value_init(a);
		value_copy(a, &r[op->a]);
	} else
	{
		*stack_push(stack) = r[op->a];
		value_reset(&r[op->a]);
	}
	op_next;

cpush_var:
	// [var, is_arg, not_null]
	b = stack_push(stack);
	if (op->b)
		a = &self->args[op->a];
	else
		a = stack_get(stack, op->a);
	if (unlikely(op->c && a->type == TYPE_NULL))
		error("variable cannot be NULL");
	value_init(b);
	value_copy(b, a);
	op_next;

cpush_nulls:
	// [count]
	for (rc = 0; rc < op->a; rc++)
		value_init(stack_push(stack));
	op_next;

cpush_bool:
	// [value, value]
	a = stack_push(stack);
	value_init(a);
	value_set_bool(a, op->a);
	op_next;

cpush_int:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_int(a, op->a);
	op_next;

cpush_double:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_double(a, code_data_at_double(code_data, op->a));
	op_next;

cpush_string:
	// [value]
	code_data_at_string(code_data, op->a, &string);
	a = stack_push(stack);
	value_init(a);
	value_set_string(a, &string, NULL);
	op_next;

cpush_json:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_decode(a, code_data_at(code_data, op->a), NULL);
	op_next;

cpush_interval:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_interval(a, (Interval*)code_data_at(code_data, op->a));
	op_next;

cpush_timestamp:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_timestamp(a, op->a);
	op_next;

cpush_date:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_date(a, op->a);
	op_next;

cpush_vector:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_vector(a, (Vector*)code_data_at(code_data, op->a), NULL);
	op_next;

cpush_uuid:
	// [value]
	a = stack_push(stack);
	value_init(a);
	value_set_uuid(a, (Uuid*)code_data_at(code_data, op->a));
	op_next;

cpush_value:
	// [value*]
	a = stack_push(stack);
	value_init(a);
	value_copy(a, (Value*)op->a);
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

cvalue:
	// [result, value*]
	b = (Value*)op->b;
	value_copy(&r[op->a], b);
	op_next;

cexcluded:
	// [result, column]
	if (unlikely(! self->upsert))
		error("unexpected EXCLUDED usage");
	b = *(Value**)(self->upsert - (sizeof(Value*) + sizeof(int64_t))) + op->b;
	if (b->type != TYPE_REF)
		value_copy(&r[op->a], b);
	else
		value_copy(&r[op->a], &self->refs[b->integer]);
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_nullu(&r[op->a], &r[op->b])))
	{
		value_set_bool(&r[op->a], !value_is_true(&r[op->b]));
		value_free(&r[op->b]);
	}
	op_next;

cborii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer | r[op->c].integer);
	op_next;

cbandii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer & r[op->c].integer);
	op_next;

cbxorii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer ^ r[op->c].integer);
	op_next;

cshlii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer << r[op->c].integer);
	op_next;

cshrii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_int(&r[op->a], r[op->b].integer >> r[op->c].integer);
	op_next;

cbinvi:
	if (likely(value_nullu(&r[op->a], &r[op->b])))
		value_set_int(&r[op->a], ~r[op->b].integer);
	op_next;

// equ
cequii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer == r[op->c].integer);
	op_next;

cequif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer == r[op->c].dbl);
	op_next;

cequfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl == r[op->c].integer);
	op_next;

cequff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl == r[op->c].dbl);
	op_next;

cequll:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], !interval_compare(&r[op->b].interval, &r[op->c].interval));
	op_next;

cequss:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !str_compare_fn(&r[op->b].string, &r[op->c].string));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cequjj:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !json_compare(r[op->b].json, r[op->c].json));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cequvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], !vector_compare(r[op->b].vector, r[op->c].vector));
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cequuu:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], !uuid_compare(&r[op->b].uuid, &r[op->c].uuid));
	op_next;

// gte
cgteii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].integer);
	op_next;

cgteif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer >= r[op->c].dbl);
	op_next;

cgtefi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl >= r[op->c].integer);
	op_next;

cgteff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl >= r[op->c].dbl);
	op_next;

cgtell:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) >= 0);
	op_next;

cgtess:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) >= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgtevv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) >= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgteuu:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) >= 0);
	op_next;

// gt
cgtii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].integer);
	op_next;

cgtif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer > r[op->c].dbl);
	op_next;

cgtfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl > r[op->c].integer);
	op_next;

cgtff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl > r[op->c].dbl);
	op_next;

cgtll:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) > 0);
	op_next;

cgtss:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) > 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgtvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) > 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cgtuu:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) > 0);
	op_next;

// lte
clteii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].integer);
	op_next;

clteif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer <= r[op->c].dbl);
	op_next;

cltefi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl <= r[op->c].integer);
	op_next;

clteff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl <= r[op->c].dbl);
	op_next;

cltell:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) <= 0);
	op_next;

cltess:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) <= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cltevv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) <= 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

clteuu:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) <= 0);
	op_next;

// lt
cltii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].integer);
	op_next;

cltif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].integer < r[op->c].dbl);
	op_next;

cltfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl < r[op->c].integer);
	op_next;

cltff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], r[op->b].dbl < r[op->c].dbl);
	op_next;

cltll:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], interval_compare(&r[op->b].interval, &r[op->c].interval) < 0);
	op_next;

cltss:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], str_compare_fn(&r[op->b].string, &r[op->c].string) < 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cltvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_set_bool(&r[op->a], vector_compare(r[op->b].vector, r[op->c].vector) < 0);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cltuu:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_bool(&r[op->a], uuid_compare(&r[op->b].uuid, &r[op->c].uuid) < 0);
	op_next;

// add
caddii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(int64_add_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int + int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

caddif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int + double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double + int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_add_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double + double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

caddtl:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, r[op->b].integer);
		timestamp_add(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

caddll:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_add(&iv, &r[op->b].interval, &r[op->c].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

caddlt:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, r[op->c].integer);
		timestamp_add(&ts, &r[op->b].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

cadddi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_add(r[op->b].integer, r[op->c].integer));
	op_next;

caddid:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_add(r[op->c].integer, r[op->b].integer));
	op_next;

cadddl:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		// convert julian to timestamp add interval and convert to unixtime
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->b].integer);
		timestamp_add(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

caddld:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->c].integer);
		timestamp_add(&ts, &r[op->b].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

caddvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_emplace(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_add(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// sub
csubii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(int64_sub_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int - int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

csubif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int - double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double - int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_sub_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double - double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

csubtl:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		timestamp_init(&ts);
		timestamp_set_unixtime(&ts, r[op->b].integer);
		timestamp_sub(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

csubtt:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_init(&iv);
		iv.us = r[op->b].integer - r[op->c].integer;
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

csubll:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		interval_sub(&iv, &r[op->b].interval, &r[op->c].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

csubdi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
		value_set_date(&r[op->a], date_sub(r[op->b].integer, r[op->c].integer));
	op_next;

csubdl:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		// convert julian to timestamp sub interval and convert to unixtime
		timestamp_init(&ts);
		timestamp_set_date(&ts, r[op->b].integer);
		timestamp_sub(&ts, &r[op->c].interval);
		value_set_timestamp(&r[op->a], timestamp_get_unixtime(&ts, NULL));
	}
	op_next;

csubvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_emplace(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_sub(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// mul
cmulii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(int64_mul_overflow(&rc, r[op->b].integer, r[op->c].integer)))
			error("int * int overflow");
		value_set_int(&r[op->a], rc);
	}
	op_next;

cmulif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int * double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cmulfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double * int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cmulff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(double_mul_overflow(&dbl, r[op->b].dbl, r[op->c].dbl)))
			error("double * double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cmulvv:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->b].vector->size != r[op->c].vector->size))
			error("vector sizes mismatch");
		buf = buf_create();
		vector = (Vector*)buf_emplace(buf, vector_size(r[op->b].vector));
		vector_init(vector, r[op->b].vector->size);
		vector_mul(vector, r[op->b].vector, r[op->c].vector);
		value_set_vector_buf(&r[op->a], buf);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

// div
cdivii:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		value_set_int(&r[op->a], r[op->b].integer / r[op->c].integer);
	}
	op_next;

cdivif:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].dbl == 0))
			error("zero division");
		if (unlikely(double_div_overflow(&dbl, r[op->b].integer, r[op->c].dbl)))
			error("int / double overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cdivfi:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		if (unlikely(double_div_overflow(&dbl, r[op->b].dbl, r[op->c].integer)))
			error("double / int overflow");
		value_set_double(&r[op->a], dbl);
	}
	op_next;

cdivff:
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null_fast(&r[op->a], &r[op->b], &r[op->c])))
	{
		if (unlikely(r[op->c].integer == 0))
			error("zero division");
		value_set_int(&r[op->a], r[op->b].integer % r[op->c].integer);
	}
	op_next;

// neg
cnegi:
	if (likely(value_nullu_fast(&r[op->a], &r[op->b])))
		value_set_int(&r[op->a], -r[op->b].integer);
	op_next;

cnegf:
	if (likely(value_nullu_fast(&r[op->a], &r[op->b])))
		value_set_double(&r[op->a], -r[op->b].dbl);
	op_next;

cnegl:
	if (likely(value_nullu_fast(&r[op->a], &r[op->b])))
	{
		interval_neg(&iv, &r[op->b].interval);
		value_set_interval(&r[op->a], &iv);
	}
	op_next;

// cat
ccatss:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
	{
		value_all(&r[op->a], &r[op->b], &r[op->c], op->d);
		value_free(&r[op->b]);
		value_free(&r[op->c]);
	}
	op_next;

cany:
	if (likely(value_null(&r[op->a], &r[op->b], &r[op->c])))
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

cset_distinct:
	// [set, cols, keys]
	// create new set and set as distinct to the parent set
	set = set_create();
	set_prepare(set, op->b, op->c, NULL);
	set_set_distinct_aggs((Set*)r[op->a].store, set);
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
	set_add(set, stack_at(stack, set->store.columns_row));
	stack_popn(stack, set->store.columns_row);
	op_next;

cset_get:
	// [result, set]
	// get existing or create new row by key,
	// return the row reference
	set = (Set*)r[op->b].store;
	rc = set_upsert(set, stack_at(stack, set->store.keys), 0, NULL);
	value_set_int(&r[op->a], rc);
	stack_popn(stack, set->store.keys);
	op_next;

cset_agg:
	// [set, row, aggs]
	set = (Set*)r[op->a].store;
	agg_write((Agg*)code_data_at(code_data, op->c),
	          stack_at(stack, set->store.columns),
	          set,
	          r[op->b].integer);
	stack_popn(stack, set->store.columns);
	op_next;

cset_agg_merge:
	// [set, aggs]
	a = &r[op->a];
	agg_merge_sets((Agg*)code_data_at(self->code_data, op->b), &a, 1);
	op_next;

cself:
	// [result, set, row, seed, agg_order]
	set = (Set*)r[op->b].store;
	// return aggregate state or seed, if the state is null
	b = set_column(set, r[op->c].integer, op->e);
	if (likely(b->type != TYPE_NULL))
		value_copy(&r[op->a], b);
	else
		value_copy(&r[op->a], &r[op->d]);
	op_next;

cunion:
	// [union, type]
	value_set_store(&r[op->a], &union_create(op->b)->store);
	op_next;

cunion_limit:
	// [union, limit]
	cunion_limit(self, op);
	op_next;

cunion_offset:
	// [union, offset]
	cunion_offset(self, op);
	op_next;

cunion_add:
	// [union, store]
	union_add((Union*)r[op->a].store, r[op->b].store);
	value_reset(&r[op->b]);
	op_next;

crecv_aggs:
	// [set, rdispatch, aggs]
	crecv_aggs(self, op);
	op_next;

crecv:
	// [union, rdispatch]
	crecv(self, op);
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

ctable_prepare:
	ctable_prepare(self, op);
	op_next;

ctable_next:
	// [cursor, _on_success]
	iterator_next(r[op->a].cursor);
	// jmp on success or skip to the next op on eof
	op = likely(iterator_has(r[op->a].cursor)) ? code_at(self->code, op->b) : op + 1;
	op_jmp;

ctable_readb:
	// [result, cursor, column]
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_bool(&r[op->a], *(int8_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi8:
	// [result, cursor, column]
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int8_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi16:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int16_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi32:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int32_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readi64:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_int(&r[op->a], *(int64_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readf32:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_double(&r[op->a], *(float*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readf64:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_double(&r[op->a], *(double*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readt:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_timestamp(&r[op->a], *(int64_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readl:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_interval(&r[op->a], (Interval*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readd:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_date(&r[op->a], *(int32_t*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_reads:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
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
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_json(&r[op->a], ptr, json_sizeof(ptr), NULL);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readv:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_vector(&r[op->a], (Vector*)ptr, NULL);
	else
		value_set_null(&r[op->a]);
	op_next;

ctable_readu:
	ptr = row_column(row_read_iterator(tr, r[op->b].cursor), op->c);
	if (likely(ptr))
		value_set_uuid(&r[op->a], (Uuid*)ptr);
	else
		value_set_null(&r[op->a]);
	op_next;

// set/merge cursor
cstore_open:
	// [cursor, store, _eof]
	a = &r[op->a];
	if (likely(r[op->b].type == TYPE_STORE))
	{
		// jmp on success or skip to the next op on eof
		a->cursor_store = store_iterator(r[op->b].store);
		if (likely(store_iterator_has(a->cursor_store)))
			op++;
		else
			op = code_at(self->code, op->c);
		a->type = TYPE_CURSOR_STORE;
	} else {
		assert(r[op->b].type == TYPE_NULL);
		value_set_null(&r[op->a]);
		op = code_at(self->code, op->c);
	}
	op_jmp;

cstore_next:
	// [cursor, _on_success]
	a = &r[op->a];
	store_iterator_next(a->cursor_store);
	// jmp on success or skip to the next op on eof
	if (likely(store_iterator_has(a->cursor_store)))
		op = code_at(self->code, op->b);
	else
		op++;
	op_jmp;

cstore_read:
	// [result, cursor, column]
	value_copy(&r[op->a], &store_iterator_at(r[op->b].cursor_store)[op->c]);
	op_next;

cjson_open:
	// [cursor, json, _eof]
	a = &r[op->a];
	if (likely(r[op->b].type == TYPE_JSON))
	{
		if (unlikely(! json_is_array(r[op->b].json)))
			error("FROM: json array expected");
		// jmp on success or skip to the next op on eof
		a->type = TYPE_CURSOR_JSON;
		a->json = r[op->b].json;
		json_read_array(&a->json);
		if (likely(! json_read_array_end(&a->json))) {
			a->json_size = json_sizeof(a->json);
			op++;
		} else {
			op = code_at(self->code, op->c);
		}
	} else {
		assert(r[op->b].type == TYPE_NULL);
		value_set_null(&r[op->a]);
		op = code_at(self->code, op->c);
	}
	op_jmp;

cjson_next:
	// [cursor, _on_success]
	a = &r[op->a];
	json_skip(&a->json);
	if (likely(! json_read_array_end(&a->json))) {
		a->json_size = json_sizeof(a->json);
		op = code_at(self->code, op->b);
	} else {
		op++;
	}
	op_jmp;

cjson_read:
	// [result, cursor]
	b = &r[op->b];
	value_set_json(&r[op->a], b->json, b->json_size, NULL);
	op_next;

cagg:
	// [result, cursor, column]
	r[op->a] = store_iterator_at(r[op->b].cursor_store)[op->c];
	op_next;

ccount:
	// [result, cursor, column]
	c = &store_iterator_at(r[op->b].cursor_store)[op->c];
	if (likely(c->type == TYPE_INT))
		value_set_int(&r[op->a], c->integer);
	else
		value_set_int(&r[op->a], 0);
	op_next;

cavgi:
	// [result, cursor, column]
	c = &store_iterator_at(r[op->b].cursor_store)[op->c];
	if (likely(c->type == TYPE_AVG))
		value_set_int(&r[op->a], avg_int(&c->avg));
	else
		value_set_null(&r[op->a]);
	op_next;

cavgf:
	// [result, cursor, column]
	c = &store_iterator_at(r[op->b].cursor_store)[op->c];
	if (likely(c->type == TYPE_AVG))
		value_set_double(&r[op->a], avg_double(&c->avg));
	else
		value_set_null(&r[op->a]);
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

ccheckpoint:
	ccheckpoint(self, op);
	op_next;

cuser_create_token:
	cuser_create_token(self, op);
	op_next;

cuser_create:
	cuser_create(self, op);
	op_next;

cuser_drop:
	cuser_drop(self, op);
	op_next;

cuser_alter:
	cuser_alter(self, op);
	op_next;

creplica_create:
	creplica_create(self, op);
	op_next;

creplica_drop:
	creplica_drop(self, op);
	op_next;

crepl_start:
	crepl_start(self, op);
	op_next;

crepl_stop:
	crepl_stop(self, op);
	op_next;

crepl_subscribe:
	crepl_subscribe(self, op);
	op_next;

crepl_unsubscribe:
	crepl_unsubscribe(self, op);
	op_next;

cddl:
	cddl(self, op);
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

cclose:
	cclose(self, op);
	op_next;

cvar:
	// [result, var, is_arg]
	if (op->c)
		b = &self->args[op->b];
	else
		b = stack_get(stack, op->b);
	value_copy(&r[op->a], b);
	op_next;

cvar_mov:
	// [var, is_arg, value]
	if (op->b)
		a = &self->args[op->a];
	else
		a = stack_get(stack, op->a);
	value_move(a, &r[op->c]);
	op_next;

cvar_set:
	// [var, is_arg, value, column]
	cvar_set(self, op);
	op_next;

cfirst:
	// [result, store]
	cfirst(self, op);
	op_next;

cref:
	// [result, id]
	value_copy(&r[op->a], &self->refs[op->b]);
	op_next;

ccall:
	// [result, function, argc, call_id]
	fn.argc     = op->c;
	fn.argv     = stack_at(stack, op->c);
	fn.result   = &r[op->a];
	fn.action   = FN_EXECUTE;
	fn.function = (Function*)op->b;
	fn.local    = self->local;
	fn.context  = NULL;
	if (op->d != -1)
		fn.context = fn_mgr_at(&self->fn_mgr, op->d);
	fn.function->function(&fn);
	stack_popn(&self->stack, op->c);
	op_next;

ccall_udf:
	// [result, udf*]
	ccall_udf(self, op);
	op_next;

cret:
	// [result]
	if (op->a != -1)
		ret->value = &r[op->a];
	return;
}
