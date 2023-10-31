
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>

void
vm_init(Vm*          self,
        Db*          db,
        FunctionMgr* function_mgr,
        Uuid*        shard)
{
	self->db           = db;
	self->function_mgr = function_mgr;
	self->trx          = NULL;
	self->code         = NULL;
	self->shard        = shard;
	self->dispatch     = NULL;
	self->command      = NULL;
	self->portal       = NULL;
	reg_init(&self->r);
	stack_init(&self->stack);
	cursor_mgr_init(&self->cursor_mgr);
}

void
vm_free(Vm* self)
{
	vm_reset(self);
	stack_free(&self->stack);
	cursor_mgr_free(&self->cursor_mgr);
}

void
vm_reset(Vm* self)
{
	reg_reset(&self->r);
	stack_reset(&self->stack);
	cursor_mgr_reset(&self->cursor_mgr);
}

#define op_start goto *ops[(op)->op]
#define op_next  goto *ops[(++op)->op]
#define op_jmp   goto *ops[(op)->op]

hot void
vm_run(Vm*          self,
       Transaction* trx,
       Dispatch*    dispatch,
       Command*     command,
       Code*        code,
       CodeData*    code_data,
       Portal*      portal)
{
	assert(code_count(code) > 0);
	self->trx       = trx;
	self->dispatch  = dispatch;
	self->command   = command;
	self->code      = code;
	self->code_data = code_data;
	self->portal    = portal;

	const void* ops[] =
	{
		&&cret,
		&&cnop,
		&&cjmp,
		&&cjtr,
		&&cjntr,
		&&csend,
		&&crecv,
		&&csleep,
		&&cpush,
		&&cpop,
		&&cnull,
		&&cbool,
		&&cint,
		&&cint_min,
		&&creal,
		&&cstring,
		&&cstring_min,
		&&cpartial,
		&&carg,
		&&cbor,
		&&cband,
		&&cbxor,
		&&cshl,
		&&cshr,
		&&cnot,
		&&cequ,
		&&cnequ,
		&&cgte,
		&&cgt,
		&&clte,
		&&clt,
		&&cadd,
		&&csub,
		&&cmul,
		&&cdiv,
		&&cmod,
		&&cneg,
		&&ccat,
		&&cidx,
		&&cmap,
		&&carray,
		&&cassign,
		&&cset,
		&&cset_ordered,
		&&cset_sort,
		&&cset_add,
		&&cset_send,
		&&cgroup,
		&&cgroup_add_aggr,
		&&cgroup_add,
		&&cgroup_get,
		&&cgroup_get_aggr,
		&&ccntr_init,
		&&ccntr_gte,
		&&ccntr_lte,

		&&ccursor_open,
		&&ccursor_open_expr,
		&&ccursor_close,
		&&ccursor_next,
		&&ccursor_read,
		&&ccursor_idx,
		&&ccall,
		&&cinsert,
		&&cupdate,
		&&cdelete
	};

	register auto stack = &self->stack;
	register auto r     =  self->r.r;
	register auto op    = code_at(self->code, 0);
	auto cursor_mgr     = &self->cursor_mgr;
	Cursor* cursor;

	int64_t rc;
	Buf*    buf;
	Str     string;
	Value*  a;
	Value*  b;
	Value*  c;
	Set*    set;
	Group*  group;

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

csend:
	buf = msg_create(MSG_OBJECT);
	value_write(&r[op->a], buf);
	msg_end(buf);
	portal_write(portal, buf);
	value_free(&r[op->a]);
	op_next;

crecv:
	dispatch_recv(self->dispatch, self->portal);
	op_next;

csleep:
	coroutine_sleep(op->a);
	op_next;

cpush:
	*stack_push(stack) = r[op->a];
	r[op->a].type = VALUE_NONE;
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

cint_min:
	value_set_int(&r[op->a], INT64_MIN);
	op_next;

creal:
	value_set_real(&r[op->a], code_data_at_real(code_data, op->b));
	op_next;

cstring:
	code_data_at_string(code_data, op->b, &string);
	value_set_string(&r[op->a], &string, NULL);
	op_next;

cstring_min:
	str_set(&string, "", 0);
	value_set_string(&r[op->a], &string, NULL);
	op_next;

cpartial:
	// todo
	//value_partial(&r[op->a], op->b, &r[op->c]);
	op_next;

carg:
	// todo
	op_next;

cbor:
	value_bor(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cband:
	value_band(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cbxor:
	value_bxor(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cshl:
	value_bshl(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cshr:
	value_bshr(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cnot:
	value_not(&r[op->a], &r[op->b]);
	value_free(&r[op->b]);
	op_next;

cequ:
	value_equ(&r[op->a], &r[op->b], &r[op->c]);
	value_free(&r[op->b]);
	value_free(&r[op->c]);
	op_next;

cnequ:
	value_nequ(&r[op->a], &r[op->b], &r[op->c]);
	value_free(&r[op->b]);
	value_free(&r[op->c]);
	op_next;

cgte:
	value_gte(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cgt:
	value_gt(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

clte:
	value_lte(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

clt:
	value_lt(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cadd:
	value_add(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

csub:
	value_sub(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cmul:
	value_mul(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cdiv:
	value_div(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cmod:
	value_mod(&r[op->a], &r[op->b], &r[op->c]);
	op_next;

cneg:
	value_neg(&r[op->a], &r[op->b]);
	op_next;

ccat:
	value_cat(&r[op->a], &r[op->b], &r[op->c]);
	value_free(&r[op->b]);
	value_free(&r[op->c]);
	op_next;

cidx:
	a = &r[op->a];
	b = &r[op->b];
	c = &r[op->c];
	value_idx(a, b, c);
	value_free(&r[op->b]);
	value_free(&r[op->c]);
	op_next;

cmap:
	value_map(&r[op->a], stack, op->b);
	stack_popn(stack, op->b);
	op_next;

carray:
	value_array(&r[op->a], stack, op->b);
	stack_popn(stack, op->b);
	op_next;

cassign:
	if (unlikely(op->b != 3))
		error("set(): incorrect call");
	a = stack_at(stack, 3);
	b = stack_at(stack, 2);
	c = stack_at(stack, 1);
	value_assign(&r[op->a], op->c, a, b, c);
	stack_popn(stack, 3);
	op_next;

cset:
	value_set_set(&r[op->a], &set_create(NULL)->obj);
	op_next;

cset_ordered:
	value_set_set(&r[op->a], &set_create(code_data_at(code_data, op->b))->obj);
	op_next;

cset_sort:
	set_sort((Set*)r[op->a].obj);
	op_next;

cset_add:
	set = (Set*)r[op->a].obj;
	set_add_from_stack(set, &r[op->b], stack);
	if (set->keys_count > 0)
		stack_popn(stack, set->keys_count);
	op_next;

cset_send:
	set = (Set*)r[op->a].obj;
	for (rc = 0; rc < set->list_count; rc++)
	{
		auto value = &set_at(set, rc)->value;
		buf = msg_create(MSG_OBJECT);
		value_write(value, buf);
		msg_end(buf);
		portal_write(portal, buf);
	}
	value_free(&r[op->a]);
	op_next;

cgroup:
	// [group, key_count]
	value_set_group(&r[op->a], &group_create(op->b)->obj);
	op_next;

cgroup_add_aggr:
	// [group, aggrype]
	group_add_aggr((Group*)r[op->a].obj, aggrs[op->b]);
	op_next;

cgroup_add:
	// get group by keys and aggregate data
	group = (Group*)r[op->a].obj;
	group_add(group, stack);
	stack_popn(stack, group->keys_count + group->aggr_count);
	op_next;

cgroup_get:
	// [result, target, pos]
	cursor = cursor_mgr_of(cursor_mgr, op->b);
	assert(cursor->type == CURSOR_GROUP);
	value_copy(&r[op->a], &group_at(cursor->group, cursor->group_pos)->keys[op->c]);
	op_next;

cgroup_get_aggr:
	// [result, target, pos]
	cursor = cursor_mgr_of(cursor_mgr, op->b);
	assert(cursor->type == CURSOR_GROUP);
	group_get_aggr(cursor->group, group_at(cursor->group, cursor->group_pos),
	               op->c, &r[op->a]);
	op_next;

ccntr_init:
	// [cursor, type, expr
	if (unlikely(r[op->c].type != VALUE_INT))
		error("LIMIT/OFFSET: integer type expected");
	cursor = cursor_mgr_of(cursor_mgr, op->a);
	if (op->b == 0)
		cursor->limit = r[op->c].integer;
	else
		cursor->offset = r[op->c].integer;
	op_next;

ccntr_gte:
	// [cursor, type, jmp]
	cursor = cursor_mgr_of(cursor_mgr, op->a);
	if (op->b == 0)
		rc = --cursor->limit;
	else
		rc = --cursor->offset;
	if (rc >= 0)
	{
		op = code_at(code, op->c);
		op_jmp;
	}
	op_next;

ccntr_lte:
	// [cursor, type, jmp]
	cursor = cursor_mgr_of(cursor_mgr, op->a);
	if (op->b == 0)
		rc = --cursor->limit;
	else
		rc = --cursor->offset;
	if (rc < 0)
	{
		op = code_at(code, op->c);
		op_jmp;
	}
	op_next;

ccursor_open:
	op = ccursor_open(self, op);
	op_jmp;

ccursor_open_expr:
	op = ccursor_open_expr(self, op);
	op_jmp;

ccursor_close:
	ccursor_close(self, op);
	op_next;

ccursor_next:
	op = ccursor_next(self, op);
	op_jmp;

ccursor_read:
	ccursor_read(self, op);
	op_next;

ccursor_idx:
	ccursor_idx(self, op);
	op_next;

ccall:
	ccall(self, op);
	op_next;

cinsert:
	cinsert(self, op);
	op_next;

cupdate:
	cupdate(self, op);
	op_next;

cdelete:
	cdelete(self, op);
	op_next;
}
