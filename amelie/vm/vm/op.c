
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

OpDesc ops[] =
{
	// control flow
	{ CNOP, "nop" },
	{ CJMP, "jmp" },
	{ CJTR, "jtr" },
	{ CJNTR, "jntr" },
	{ CJGTED, "cjgted" },
	{ CJLTD, "cjltd" },

	// values
	{ CFREE, "free" },
	{ CDUP, "dup" },
	{ CMOV, "mov" },

	// stack
	{ CPUSH, "push" },
	{ CPUSH_REF, "push_ref" },
	{ CPUSH_VAR, "push_var" },
	{ CPUSH_NULLS, "push_nulls" },
	{ CPUSH_BOOL, "push_bool" },
	{ CPUSH_INT, "push_int" },
	{ CPUSH_DOUBLE, "push_double" },
	{ CPUSH_STRING, "push_string" },
	{ CPUSH_JSON, "push_json" },
	{ CPUSH_INTERVAL, "push_interval" },
	{ CPUSH_TIMESTAMP, "push_timestamp" },
	{ CPUSH_DATE, "push_date" },
	{ CPUSH_VECTOR, "push_vector" },
	{ CPUSH_UUID, "push_uuid" },
	{ CPUSH_VALUE, "push_value" },
	{ CPOP, "pop" },

	// consts
	{ CNULL, "null" },
	{ CBOOL, "bool" },
	{ CINT, "int" },
	{ CDOUBLE, "double" },
	{ CSTRING, "string" },
	{ CJSON, "json" },
	{ CJSON_OBJ, "json_obj" },
	{ CJSON_ARRAY, "json_array" },
	{ CINTERVAL, "interval" },
	{ CTIMESTAMP, "timestamp" },
	{ CDATE, "date" },
	{ CVECTOR, "vector" },
	{ CUUID, "uuid" },
	{ CVALUE, "value" },

	// argument
	{ CEXCLUDED, "excluded" },

	// null operations
	{ CNULLOP, "nullop" },
	{ CIS, "is" },

	// logical (any)
	{ CAND, "and" },
	{ COR, "or" },
	{ CNOT, "not" },

	// bitwise operations
	{ CBORII, "borii" },
	{ CBANDII, "bandii" },
	{ CBXORII, "bxorii" },
	{ CBSHLII, "bshlii" },
	{ CBSHRII, "bshrii" },
	{ CBINVI, "binvi" },

	// equ
	{ CEQUII, "equii" },
	{ CEQUIF, "equif" },
	{ CEQUFI, "equfi" },
	{ CEQUFF, "equff" },
	{ CEQULL, "equll" },
	{ CEQUSS, "equss" },
	{ CEQUJJ, "equjj" },
	{ CEQUVV, "equvv" },
	{ CEQUUU, "equuu" },

	// gte
	{ CGTEII, "gteii" },
	{ CGTEIF, "gteif" },
	{ CGTEFI, "gtefi" },
	{ CGTEFF, "gteff" },
	{ CGTELL, "gtell" },
	{ CGTESS, "gtess" },
	{ CGTEVV, "gtevv" },
	{ CGTEUU, "gteuu" },

	// gt
	{ CGTII, "gtii" },
	{ CGTIF, "gtif" },
	{ CGTFI, "gtfi" },
	{ CGTFF, "gtff" },
	{ CGTLL, "gtll" },
	{ CGTSS, "gtss" },
	{ CGTVV, "gtvv" },
	{ CGTUU, "gtuu" },

	// lte
	{ CLTEII, "lteii" },
	{ CLTEIF, "lteif" },
	{ CLTEFI, "ltefi" },
	{ CLTEFF, "lteff" },
	{ CLTELL, "ltell" },
	{ CLTESS, "ltess" },
	{ CLTEVV, "ltevv" },
	{ CLTEUU, "lteuu" },

	// lt
	{ CLTII, "ltii" },
	{ CLTIF, "ltif" },
	{ CLTFI, "ltfi" },
	{ CLTFF, "ltff" },
	{ CLTLL, "ltll" },
	{ CLTSS, "ltss" },
	{ CLTVV, "ltvv" },
	{ CLTUU, "ltuu" },

	// add
	{ CADDII, "addii" },
	{ CADDIF, "addif" },
	{ CADDFI, "addfi" },
	{ CADDFF, "addff" },
	{ CADDTL, "addtl" },
	{ CADDLL, "addll" },
	{ CADDLT, "addlt" },
	{ CADDDI, "adddi" },
	{ CADDID, "addid" },
	{ CADDDL, "adddl" },
	{ CADDLD, "addld" },
	{ CADDVV, "addvv" },

	// sub
	{ CSUBII, "subii" },
	{ CSUBIF, "subif" },
	{ CSUBFI, "subfi" },
	{ CSUBFF, "subff" },
	{ CSUBTL, "subtl" },
	{ CSUBTT, "subtt" },
	{ CSUBLL, "subll" },
	{ CSUBDI, "subdi" },
	{ CSUBDL, "subdl" },
	{ CSUBVV, "subvv" },

	// mul
	{ CMULII, "mulii" },
	{ CMULIF, "mulif" },
	{ CMULFI, "mulfi" },
	{ CMULFF, "mulff" },
	{ CMULVV, "mulvv" },

	// div
	{ CDIVII, "divii" },
	{ CDIVIF, "divif" },
	{ CDIVFI, "divfi" },
	{ CDIVFF, "divff" },

	// mod
	{ CMODII, "modii" },

	// neg
	{ CNEGI, "negi" },
	{ CNEGF, "negf" },
	{ CNEGL, "negl" },

	// cat
	{ CCATSS, "catss" },

	// idx
	{ CIDXJS, "idxjs" },
	{ CIDXJI, "idxji" },
	{ CIDXVI, "idxvi" },

	// dot
	{ CDOTJS, "dotjs" },

	// like
	{ CLIKESS, "likess" },

	{ CIN, "in" },
	{ CALL, "all" },
	{ CANY, "any" },
	{ CEXISTS, "exists" },

	// set
	{ CSET, "set" },
	{ CSET_ORDERED, "set_ordered" },
	{ CSET_DISTINCT, "set_distinct" },
	{ CSET_PTR, "set_ptr" },
	{ CSET_SORT, "set_sort" },
	{ CSET_ADD, "set_add" },
	{ CSET_GET, "set_get" },
	{ CSET_AGG, "set_agg" },
	{ CSET_AGG_MERGE, "set_agg_merge" },
	{ CSELF, "self" },

	// union
	{ CUNION, "union" },
	{ CUNION_LIMIT, "union_limit" },
	{ CUNION_OFFSET, "union_offset" },
	{ CUNION_ADD, "union_add" },
	{ CRECV_AGGS, "recv_aggs" },
	{ CRECV, "recv" },

	// table cursor
	{ CTABLE_OPEN, "table_open" },
	{ CTABLE_OPENL, "table_openl" },
	{ CTABLE_OPEN_PART, "table_open_part" },
	{ CTABLE_OPEN_PARTL, "table_open_partl" },
	{ CTABLE_OPEN_HEAP, "table_open_heap" },
	{ CTABLE_PREPARE, "table_prepare" },
	{ CTABLE_NEXT, "table_next" },
	{ CTABLE_READB, "table_readb" },
	{ CTABLE_READI8, "table_readi8" },
	{ CTABLE_READI16, "table_readi16" },
	{ CTABLE_READI32, "table_readi32" },
	{ CTABLE_READI64, "table_readi64" },
	{ CTABLE_READF32, "table_readf32" },
	{ CTABLE_READF64, "table_readf64" },
	{ CTABLE_READT, "table_readt" },
	{ CTABLE_READL, "table_readl" },
	{ CTABLE_READD, "table_readd" },
	{ CTABLE_READS, "table_reads" },
	{ CTABLE_READJ, "table_readj" },
	{ CTABLE_READV, "table_readv" },
	{ CTABLE_READU, "table_readu" },

	// store cursor
	{ CSTORE_OPEN, "store_open" },
	{ CSTORE_NEXT, "store_next" },
	{ CSTORE_READ, "store_read" },

	// json cursor
	{ CJSON_OPEN, "json_open" },
	{ CJSON_NEXT, "json_next" },
	{ CJSON_READ, "json_read" },

	// aggs
	{ CAGG, "agg" },
	{ CCOUNT, "count" },
	{ CAVGI, "avgi" },
	{ CAVGF, "avgf" },

	// dml
	{ CINSERT, "insert" },
	{ CUPSERT, "upsert" },
	{ CDELETE, "delete" },
	{ CUPDATE, "update" },
	{ CUPDATE_STORE, "update_store" },

	// system
	{ CCHECKPOINT, "checkpoint" },

	// user
	{ CUSER_CREATE_TOKEN, "user_create_token" },
	{ CUSER_CREATE, "user_create" },
	{ CUSER_DROP, "user_drop" },
	{ CUSER_ALTER, "user_alter" },

	// replica
	{ CREPLICA_CREATE, "replica_create" },
	{ CREPLICA_DROP, "replica_drop" },

	// replication
	{ CREPL_START, "repl_start" },
	{ CREPL_STOP, "repl_stop" },
	{ CREPL_SUBSCRIBE, "repl_subscribe" },
	{ CREPL_UNSUBSCRIBE, "repl_unsubscribe" },

	// ddl
	{ CDDL, "ddl" },

	// result
	{ CSEND_SHARD, "send_shard" },
	{ CSEND_LOOKUP, "send_lookup" },
	{ CSEND_LOOKUP_BY, "send_lookup_by" },
	{ CSEND_ALL, "send_all" },
	{ CCLOSE, "close" },

	// var
	{ CVAR, "var" },
	{ CVAR_MOV, "var_mov" },
	{ CVAR_SET, "var_set" },
	{ CFIRST, "first" },
	{ CREF, "ref" },

	// call / return
	{ CCALL, "call" },
	{ CCALL_UDF, "call_udf" },
	{ CRET, "ret" },

	{ 0, NULL }
};

static inline void
op_write(Buf* output, Op* op, bool a, bool b, bool c, char *fmt, ...)
{
	buf_printf(output, "%-20s", ops[op->op].name);

	if (a)
		buf_printf(output, "%-6" PRIi64 " ", op->a);
	else
		buf_printf(output, "%-6s ", "-");

	if (b)
		buf_printf(output, "%-6" PRIi64 " ", op->b);
	else
		buf_printf(output, "%-6s ", "-");

	if (c)
		buf_printf(output, "%-6" PRIi64, op->c);
	else
		buf_printf(output, "%-6s", "-");

	if (fmt)
	{
		buf_printf(output, "# ");
		va_list args;
		va_start(args, fmt);
		buf_vprintf(output, fmt, args);
		va_end(args);
	}
}

static void
op_dump_send(Program* self, Code* code, Op* op, Buf* buf, Send* send,
             bool a, bool b, bool c)
{
	auto is_last = self->send_last == code_posof(code, op);
	auto config = send->table->config;
	op_write(buf, op, a, b, c,
	         "%.*s (%s%s)",
	         str_size(&config->name), str_of(&config->name),
	         send_of(send),
	         is_last ? ", closing" : "");
}

void
op_dump(Program* self, Code* code, Buf* buf)
{
	auto data   = &self->code_data;
	auto output = buf_create();
	defer_buf(output);

	encode_obj(buf);
	auto op  = (Op*)code->code.start;
	auto end = (Op*)code->code.position;
	for (int pos = 0; op < end; pos++)
	{
		// "pos": "op description"
		char pos_sz[32];
		int  pos_sz_len = sfmt(pos_sz, sizeof(pos_sz), "%02d", pos);
		encode_raw(buf, pos_sz, pos_sz_len);

		buf_reset(output);
		switch (op->op) {
		case CPUSH_INT:
		case CPUSH_TIMESTAMP:
		{
			op_write(output, op, false, true, true, "%" PRIi64, op->a);
			break;
		}
		case CINT:
		case CTIMESTAMP:
		{
			op_write(output, op, true, false, true, "%" PRIi64, op->b);
			break;
		}
		case CPUSH_STRING:
		{
			Str str;
			code_data_at_string(data, op->a, &str);
			op_write(output, op, true, true, true, "%.*s",
			         str_size(&str), str_of(&str));
			break;
		}
		case CSTRING:
		{
			Str str;
			code_data_at_string(data, op->b, &str);
			op_write(output, op, true, true, true, "%.*s",
			         str_size(&str), str_of(&str));
			break;
		}
		case CPUSH_DOUBLE:
		{
			double dbl = code_data_at_double(data, op->a);
			op_write(output, op, true, true, true, "%g", dbl);
			break;
		}
		case CDOUBLE:
		{
			double dbl = code_data_at_double(data, op->b);
			op_write(output, op, true, true, true, "%g", dbl);
			break;
		}

		case CPUSH_VALUE:
		{
			auto value = (Value*)op->a;
			op_write(output, op, false, true, true, "%s",
			         type_of(value->type));
			break;
		}

		case CSET_PTR:
			op_write(output, op, true, false, true, NULL);
			break;
		case CSEND_SHARD:
			op_dump_send(self, code, op, output, send_at(data, op->d),
			             true, true, true);
			break;
		case CSEND_LOOKUP:
			op_dump_send(self, code, op, output, send_at(data, op->d),
			             true, false, true);
			break;
		case CSEND_LOOKUP_BY:
		case CSEND_ALL:
			op_dump_send(self, code, op, output, send_at(data, op->c),
			             true, true, true);
			break;
		case CINSERT:
		{
			auto table = (Table*)op->a;
			op_write(output, op, false, true, true,
			         "%.*s", str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CTABLE_OPEN:
		case CTABLE_OPENL:
		case CTABLE_OPEN_PART:
		case CTABLE_OPEN_PARTL:
		{
			Str name_db;
			Str name_table;
			Str name_index;
			uint8_t* ref = code_data_at(data, op->b);
			json_read_string(&ref, &name_db);
			json_read_string(&ref, &name_table);
			json_read_string(&ref, &name_index);
			op_write(output, op, true, true, true,
			         "%.*s (%.*s)",
			         str_size(&name_table),
			         str_of(&name_table),
			         str_size(&name_index),
			         str_of(&name_index));
			break;
		}
		case CTABLE_OPEN_HEAP:
		{
			Str name_db;
			Str name_table;
			uint8_t* ref = code_data_at(data, op->b);
			json_read_string(&ref, &name_db);
			json_read_string(&ref, &name_table);
			op_write(output, op, true, true, true,
			         "%.*s", str_size(&name_table),
			         str_of(&name_table));
			break;
		}
		case CTABLE_PREPARE:
		{
			auto table = (Table*)op->b;
			op_write(output, op, true, false, true,
			         "%.*s", str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CCALL:
		{
			auto function = (Function*)op->b;
			op_write(output, op, true, false, true,
			         "%.*s()", str_size(&function->name),
			         str_of(&function->name));
			break;
		}
		case CCALL_UDF:
		{
			auto udf = (Udf*)op->b;
			op_write(output, op, true, false, false,
			         "%.*s()", str_size(&udf->config->name),
			         str_of(&udf->config->name));
			break;
		}
		case CVALUE:
		{
			auto value = (Value*)op->b;
			op_write(output, op, true, false, true, "%s",
			         type_of(value->type));
			break;
		}
		case CRET:
			op_write(output, op, true, true, false, NULL);
			break;
		default:
			op_write(output, op, true, true, true, NULL);
			break;
		}
		encode_raw(buf, (char*)output->start, buf_size(output));
		op++;
	}

	encode_obj_end(buf);
}
