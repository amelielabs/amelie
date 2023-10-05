
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

static inline void
emit_op(Compiler* self, int op)
{
	int r = rmap_pop(&self->map);
	int l = rmap_pop(&self->map);
	int o = op3(self, op, rpin(self), l, r);
	rmap_push(&self->map, o);
	runpin(self, l);
	runpin(self, r);
}

static inline void
emit_unary(Compiler* self, int op)
{
	int l = rmap_pop(&self->map);
	int o = op2(self, op, rpin(self), l);
	rmap_push(&self->map, o);
	runpin(self, l);
}

#if 0
		case KSET:
			return expr_generate_call(self, target, ast, COBJ_SET);
		case KUNSET:
			return expr_generate_call(self, target, ast, COBJ_UNSET);
		case KHAS:
			return expr_generate_call(self, target, ast, COBJ_HAS);
		case KSTRING:
			return expr_generate_call(self, target, ast, CTO_STRING);
		case KJSON:
			return expr_generate_call(self, target, ast, CTO_JSON);
		case KSIZEOF:
			return expr_generate_call(self, target, ast, CSIZEOF);

		// aggregates
		case KCOUNT:
		case KSUM:
		case KAVG:
			return expr_generate_aggregate(self, ast);
#endif

static inline void
emit_call(Compiler* self, Ast* ast)
{
	auto call = ast_call_of(ast);
	// todo: check name?

	for (int i = 0; i < call->count; i++)
	{
		int r = rmap_pop(&self->map);
		op1(self, CPUSH, r);
		runpin(self, r);
	}

	// push name
	int name_offset;
	name_offset = code_add_string(self->code, &call->name->string);
	int o = op3(self, CCALL, rpin(self), name_offset, call->count);
	rmap_push(&self->map, o);
}

static inline void
emit_object(Compiler* self, Ast* ast, int op)
{
	auto call = ast_call_of(ast);
	for (int i = 0; i < call->count; i++)
	{
		int r = rmap_pop(&self->map);
		op1(self, CPUSH, r);
		runpin(self, r);
	}
	int o = op2(self, op, rpin(self), call->count);
	rmap_push(&self->map, o);
}

static inline void
emit_name(Compiler* self, Target* target, Ast* ast)
{
	(void)self;
	(void)target;
	(void)ast;
}

static inline void
emit_name_compound(Compiler* self, Target* target, Ast* ast)
{
	(void)self;
	(void)target;
	(void)ast;
}

static inline void
emit_name_compound_star(Compiler* self, Target* target, Ast* ast)
{
	(void)self;
	(void)target;
	(void)ast;
}

static inline void
emit_star(Compiler* self, Target* target, Ast* ast)
{
	(void)self;
	(void)target;
	(void)ast;
}

hot void
emit_expr(Compiler* self, Target* target, Ast* expr)
{
	(void)self;
	(void)expr;

	for (auto e = expr; e; e = e->next)
	{
		switch (e->id) {
		// logic
		case KOR:
		case KAND:
			// todo
			break;

		// ops
		case KEQ:
			emit_op(self, CEQU);
			break;
		case KNEQ:
			emit_op(self, CNEQU);
			break;
		case KGTE:
			emit_op(self, CGTE);
			break;
		case KLTE:
			emit_op(self, CLTE);
			break;
		case KGT:
			emit_op(self, CGT);
			break;
		case KLT:
			emit_op(self, CLT);
			break;
		case KBOR:
			emit_op(self, CBOR);
			break;
		case KXOR:
			emit_op(self, CBXOR);
			break;
		case KBAND:
			emit_op(self, CBAND);
			break;
		case KSHL:
			emit_op(self, CSHL);
			break;
		case KSHR:
			emit_op(self, CSHR);
			break;
		case KADD:
			emit_op(self, CADD);
			break;
		case KSUB:
			emit_op(self, CSUB);
			break;
		case KCAT:
			emit_op(self, CCAT);
			break;
		case KMUL:
			emit_op(self, CMUL);
			break;
		case KDIV:
			emit_op(self, CDIV);
			break;
		case KMOD:
			emit_op(self, CMOD);
			break;
		case KIDX:
			emit_op(self, CIDX);
			break;

		// unary operations
		case KNEG:
			emit_unary(self, CNEG);
			break;
		case KNOT:
			emit_unary(self, CNOT);
			break;

		// call function()
		case KRBL:
			emit_call(self, e);
			break;

		// object
		case KCBL:
			emit_object(self, e, CMAP);
			break;
		case KSBL:
			emit_object(self, e, CARRAY);
			break;

		// value
		case KNULL:
		{
			int r = op1(self, CNULL, rpin(self));
			rmap_push(&self->map, r);
			break;
		}
		case KINT:
		{
			int r = op2(self, CINT, rpin(self), e->integer);
			rmap_push(&self->map, r);
			break;
		}
		case KFLOAT:
		{
			int r = op2(self, CFLOAT, rpin(self), code_add_fp(self->code, e->fp));
			rmap_push(&self->map, r);
			break;
		}
		case KTRUE:
		{
			int r = op2(self, CBOOL, rpin(self), 1);
			rmap_push(&self->map, r);
			break;
		}
		case KFALSE:
		{
			int r = op2(self, CBOOL, rpin(self), 0);
			rmap_push(&self->map, r);
			break;
		}

		case KSTRING:
		{
			int offset;
			if (e->string_escape)
				offset = code_add_string(self->code, &e->string);
			else
				offset = code_add_string_unescape(self->code, &e->string);
			int r = op2(self, CSTRING, rpin(self), offset);
			rmap_push(&self->map, r);
			break;
		}

		// argument
		case KARGUMENT:
		{
			int r = op2(self, CARG, rpin(self), e->integer);
			rmap_push(&self->map, r);
			break;
		}

		case KARGUMENT_NAME:
			// todo
			break;

		// name
		case KNAME:
			emit_name(self, target, e);
			return;

		case KNAME_COMPOUND:
			emit_name_compound(self, target, e);
			break;

		case KNAME_COMPOUND_STAR:
			emit_name_compound_star(self, target, e);
			break;

		case KSTAR:
			emit_star(self, target, e);
			break;

		// sub-query
		case KSELECT:
			// todo
			break;

		default:
			assert(0);
			break;
		}
	}
}
