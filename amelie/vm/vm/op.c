
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

OpDesc ops[] =
{
	// control
	{ CRET, "ret" },
	{ CNOP, "nop" },
	{ CJMP, "jmp" },
	{ CJTR, "jtr" },
	{ CJNTR, "jntr" },
	{ CJGTED, "cjgted" },
	{ CJLTD, "cjltd" },
	{ CSWAP, "swap" },

	// stack
	{ CPUSH, "push" },
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

	// argument
	{ CARG, "arg" },
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
	{ CEQUID, "equid" },
	{ CEQUDI, "equdi" },
	{ CEQUDD, "equdd" },
	{ CEQULL, "equll" },
	{ CEQUSS, "equss" },
	{ CEQUJJ, "equjj" },
	{ CEQUVV, "equvv" },

	// gte
	{ CGTEII, "gteii" },
	{ CGTEID, "gteid" },
	{ CGTEDI, "gtedi" },
	{ CGTEDD, "gtedd" },
	{ CGTELL, "gtell" },
	{ CGTESS, "gtess" },
	{ CGTEVV, "gtevv" },

	// gt
	{ CGTII, "gtii" },
	{ CGTID, "gtid" },
	{ CGTDI, "gtdi" },
	{ CGTDD, "gtdd" },
	{ CGTLL, "gtll" },
	{ CGTSS, "gtss" },
	{ CGTVV, "gtvv" },

	// lte
	{ CLTEII, "lteii" },
	{ CLTEID, "lteid" },
	{ CLTEDI, "ltedi" },
	{ CLTEDD, "ltedd" },
	{ CLTELL, "ltell" },
	{ CLTESS, "ltess" },
	{ CLTEVV, "ltevv" },

	// lt
	{ CLTII, "ltii" },
	{ CLTID, "ltid" },
	{ CLTDI, "ltdi" },
	{ CLTDD, "ltdd" },
	{ CLTLL, "ltll" },
	{ CLTSS, "ltss" },
	{ CLTVV, "ltvv" },

	// add
	{ CADDII, "addii" },
	{ CADDID, "addid" },
	{ CADDDI, "adddi" },
	{ CADDDD, "adddd" },
	{ CADDTL, "addtl" },
	{ CADDLL, "addll" },
	{ CADDLT, "addlt" },
	{ CADDVV, "addvv" },

	// sub
	{ CSUBII, "subii" },
	{ CSUBID, "subid" },
	{ CSUBDI, "subdi" },
	{ CSUBDD, "subdd" },
	{ CSUBTL, "subtl" },
	{ CSUBLL, "subll" },
	{ CSUBVV, "subvv" },

	// mul
	{ CMULII, "mulii" },
	{ CMULID, "mulid" },
	{ CMULDI, "muldi" },
	{ CMULDD, "muldd" },
	{ CMULVV, "mulvv" },

	// div
	{ CDIVII, "divii" },
	{ CDIVID, "divid" },
	{ CDIVDI, "divdi" },
	{ CDIVDD, "divdd" },

	// mod
	{ CMODII, "modii" },

	// neg
	{ CNEGI, "negi" },
	{ CNEGD, "negd" },

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
	{ CSET_PTR, "set_ptr" },
	{ CSET_SORT, "set_sort" },
	{ CSET_ADD, "set_add" },
	{ CSET_GET, "set_get" },
	{ CSET_RESULT, "set_result" },
	{ CSET_AGG, "set_agg" },

	// merge
	{ CMERGE, "merge" },
	{ CMERGE_RECV, "merge_recv" },
	{ CMERGE_RECV_AGG, "merge_recv_agg" },

	// table cursor
	{ CTABLE_OPEN, "table_open" },
	{ CTABLE_PREPARE, "table_prepare" },
	{ CTABLE_CLOSE, "table_close" },
	{ CTABLE_NEXT, "table_next" },
	{ CTABLE_READB, "table_readb" },
	{ CTABLE_READI8, "table_readi8" },
	{ CTABLE_READI16, "table_readi16" },
	{ CTABLE_READI32, "table_readi32" },
	{ CTABLE_READI64, "table_readi64" },
	{ CTABLE_READF, "table_readf" },
	{ CTABLE_READD, "table_readd" },
	{ CTABLE_READT, "table_readt" },
	{ CTABLE_READL, "table_readl" },
	{ CTABLE_READS, "table_reads" },
	{ CTABLE_READJ, "table_readj" },
	{ CTABLE_READV, "table_readv" },

	// store cursor
	{ CSTORE_OPEN, "store_open" },
	{ CSTORE_CLOSE, "store_open" },
	{ CSTORE_NEXT, "store_next" },
	{ CSTORE_READ, "store_read" },

	// aggs
	{ CAGG, "agg" },
	{ CCOUNT, "count" },
	{ CAVGI, "avgi" },
	{ CAVGD, "avgd" },

	// functions
	{ CCALL, "call" },

	// dml
	{ CINSERT, "insert" },
	{ CUPSERT, "upsert" },
	{ CUPDATE, "update" },
	{ CDELETE, "delete" },

	// result
	{ CSEND, "send" },
	{ CSEND_HASH, "send_hash" },
	{ CSEND_GENERATED, "send_generated" },
	{ CSEND_FIRST, "send_first" },
	{ CSEND_ALL, "send_all" },
	{ CRECV, "recv" },
	{ CRECV_TO, "recv_to" },

	// result
	{ CRESULT, "result" },
	{ CCONTENT, "content" },
	{ CCTE_SET, "cte_set" },
	{ CCTE_GET, "cte_get" },

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

void
op_dump(Code* self, CodeData* data, Buf* buf)
{
	auto output = buf_create();
	guard_buf(output);

	encode_obj(buf);
	auto op  = (Op*)self->code.start;
	auto end = (Op*)self->code.position;
	for (int pos = 0; op < end; pos++)
	{
		// "pos": "op description"
		char pos_sz[32];
		int  pos_sz_len = snprintf(pos_sz, sizeof(pos_sz), "%02d", pos);
		encode_raw(buf, pos_sz, pos_sz_len);

		buf_reset(output);
		switch (op->op) {
		case CINT:
		case CTIMESTAMP:
		{
			op_write(output, op, true, false, true, "%" PRIi64, op->b);
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
		case CDOUBLE:
		{
			double dbl = code_data_at_double(data, op->b);
			op_write(output, op, true, true, true, "%g", dbl);
			break;
		}
		case CSET_PTR:
			op_write(output, op, true, false, true, NULL);
			break;
		case CSEND_HASH:
		case CSEND:
		case CSEND_GENERATED:
		case CSEND_ALL:
		{
			auto table = (Table*)op->c;
			op_write(output, op, true, true, false,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CINSERT:
		{
			auto table = (Table*)op->a;
			op_write(output, op, false, true, true,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CTABLE_OPEN:
		{
			Str schema;
			Str table;
			Str index;
			uint8_t* ref = code_data_at(data, op->b);
			json_read_string(&ref, &schema);
			json_read_string(&ref, &table);
			json_read_string(&ref, &index);
			op_write(output, op, true, true, true,
			         "%.*s.%.*s (%.*s%s)",
			         str_size(&schema),
			         str_of(&schema),
			         str_size(&table),
			         str_of(&table),
			         str_size(&index),
			         str_of(&index),
			         op->d ? ", lookup": "");
			break;
		}
		case CTABLE_PREPARE:
		{
			auto table = (Table*)op->b;
			op_write(output, op, true, false, true,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CCALL:
		{
			auto function = (Function*)op->b;
			op_write(output, op,true, false, true,
			         "%.*s.%.*s()",
			         str_size(&function->schema),
			         str_of(&function->schema),
			         str_size(&function->name),
			         str_of(&function->name));
			break;
		}
		case CCONTENT:
			op_write(output, op, true, false, false, NULL);
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
