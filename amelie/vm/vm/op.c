
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

	// values
	{ CFREE, "free" },
	{ CDUP, "dup" },
	{ CMOV, "mov" },

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
	{ CDATE, "date" },
	{ CVECTOR, "vector" },
	{ CUUID, "uuid" },

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
	{ CSET_ASSIGN, "set_assign" },
	{ CSET_PTR, "set_ptr" },
	{ CSET_SORT, "set_sort" },
	{ CSET_ADD, "set_add" },
	{ CSET_GET, "set_get" },
	{ CSET_RESULT, "set_result" },
	{ CSET_AGG, "set_agg" },
	{ CSELF, "self" },

	// union
	{ CUNION, "union" },
	{ CUNION_SET_AGGS, "union_set_aggs" },
	{ CUNION_RECV, "union_recv" },

	// table cursor
	{ CTABLE_OPEN, "table_open" },
	{ CTABLE_OPENL, "table_openl" },
	{ CTABLE_OPEN_PART, "table_open_part" },
	{ CTABLE_OPEN_PARTL, "table_open_partl" },
	{ CTABLE_OPEN_HEAP, "table_open_heap" },
	{ CTABLE_PREPARE, "table_prepare" },
	{ CTABLE_CLOSE, "table_close" },
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
	{ CSTORE_CLOSE, "store_close" },
	{ CSTORE_NEXT, "store_next" },
	{ CSTORE_READ, "store_read" },

	// json cursor
	{ CJSON_OPEN, "json_open" },
	{ CJSON_CLOSE, "json_close" },
	{ CJSON_NEXT, "json_next" },
	{ CJSON_READ, "json_read" },

	// aggs
	{ CAGG, "agg" },
	{ CCOUNT, "count" },
	{ CAVGI, "avgi" },
	{ CAVGF, "avgf" },

	// functions
	{ CCALL, "call" },

	// dml
	{ CINSERT, "insert" },
	{ CUPSERT, "upsert" },
	{ CDELETE, "delete" },
	{ CUPDATE, "update" },
	{ CUPDATE_STORE, "update_store" },

	// result
	{ CSEND_SHARD, "send_shard" },
	{ CSEND_LOOKUP, "send_lookup" },
	{ CSEND_LOOKUP_BY, "send_lookup_by" },
	{ CSEND_ALL, "send_all" },
	{ CRECV, "recv" },

	// result
	{ CASSIGN, "assign" },
	{ CRESULT, "result" },
	{ CCONTENT, "content" },
	{ CREF, "ref" },

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
	defer_buf(output);

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
		case CSEND_SHARD:
		case CSEND_ALL:
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
		case CSEND_LOOKUP:
		case CSEND_LOOKUP_BY:
		{
			auto table = (Table*)op->b;
			op_write(output, op, true, false, false,
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
		case CTABLE_OPENL:
		case CTABLE_OPEN_PART:
		case CTABLE_OPEN_PARTL:
		{
			Str name_schema;
			Str name_table;
			Str name_index;
			uint8_t* ref = code_data_at(data, op->b);
			json_read_string(&ref, &name_schema);
			json_read_string(&ref, &name_table);
			json_read_string(&ref, &name_index);
			op_write(output, op, true, true, true,
			         "%.*s.%.*s (%.*s)",
			         str_size(&name_schema),
			         str_of(&name_schema),
			         str_size(&name_table),
			         str_of(&name_table),
			         str_size(&name_index),
			         str_of(&name_index));
			break;
		}
		case CTABLE_OPEN_HEAP:
		{
			Str name_schema;
			Str name_table;
			uint8_t* ref = code_data_at(data, op->b);
			json_read_string(&ref, &name_schema);
			json_read_string(&ref, &name_table);
			op_write(output, op, true, true, true,
			         "%.*s.%.*s",
			         str_size(&name_schema),
			         str_of(&name_schema),
			         str_size(&name_table),
			         str_of(&name_table));
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
