
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
#include <amelie_data.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>

OpDesc ops[] =
{
	// control
	{ CRET, "cret" },
	{ CNOP, "cnop" },
	{ CJMP, "cjmp" },
	{ CJTR, "cjtr" },
	{ CJNTR, "cjntr" },
	{ CSWAP, "cswap" },

	// stack
	{ CPUSH, "cpush" },
	{ CPOP, "cpop" },

	// consts
	{ CNULL, "cnull" },
	{ CBOOL, "cbool" },
	{ CINT, "cint" },
	{ CINT_MIN, "cint_min" },
	{ CDOUBLE, "cdouble" },
	{ CSTRING, "cstring" },
	{ CJSON, "cjson" },
	{ CJSON_OBJ, "cjson_obj" },
	{ CJSON_ARRAY, "cjson_array" },
	{ CINTERVAL, "cinterval" },
	{ CTIMESTAMP, "ctimestamp" },
	{ CSTRING_MIN, "cstring_min" },
	{ CTIMESTAMP_MIN, "ctimestamp_min" },

	// argument
	{ CARG, "carg" },

	// logical (any)
	{ CNOT, "cnot" },

	// bitwise operations
	{ CBORII, "cborii" },
	{ CBANDII, "cbandii" },
	{ CBXORII, "cbxorii" },
	{ CBSHLII, "cbshlii" },
	{ CBSHRII, "cbshrii" },
	{ CBINVI, "cbinvi" },

	// equ
	{ CEQUII, "cequii" },
	{ CEQUID, "cequid" },
	{ CEQUDI, "cequdi" },
	{ CEQUDD, "cequdd" },
	{ CEQULL, "cequll" },
	{ CEQUSS, "cequss" },
	{ CEQUJJ, "cequjj" },
	{ CEQUVV, "cequvv" },

	// gte
	{ CGTEII, "cgteii" },
	{ CGTEID, "cgteid" },
	{ CGTEDI, "cgtedi" },
	{ CGTEDD, "cgtedd" },
	{ CGTELL, "cgtell" },
	{ CGTESS, "cgtess" },
	{ CGTEVV, "cgtevv" },

	// gt
	{ CGTII, "cgtii" },
	{ CGTID, "cgtid" },
	{ CGTDI, "cgtdi" },
	{ CGTDD, "cgtdd" },
	{ CGTLL, "cgtll" },
	{ CGTSS, "cgtss" },
	{ CGTVV, "cgtvv" },

	// lte
	{ CLTEII, "clteii" },
	{ CLTEID, "clteid" },
	{ CLTEDI, "cltedi" },
	{ CLTEDD, "cltedd" },
	{ CLTELL, "cltell" },
	{ CLTESS, "cltess" },
	{ CLTEVV, "cltevv" },

	// lt
	{ CLTII, "cltii" },
	{ CLTID, "cltid" },
	{ CLTDI, "cltdi" },
	{ CLTDD, "cltdd" },
	{ CLTLL, "cltll" },
	{ CLTSS, "cltss" },
	{ CLTVV, "cltvv" },

	// add
	{ CADDII, "caddii" },
	{ CADDID, "caddid" },
	{ CADDDI, "cadddi" },
	{ CADDDD, "cadddd" },
	{ CADDTL, "caddtl" },
	{ CADDLL, "caddll" },
	{ CADDLT, "caddlt" },
	{ CADDVV, "caddvv" },

	// sub
	{ CSUBII, "csubii" },
	{ CSUBID, "csubid" },
	{ CSUBDI, "csubdi" },
	{ CSUBDD, "csubdd" },
	{ CSUBTL, "csubtl" },
	{ CSUBLL, "csubll" },
	{ CSUBLT, "csublt" },
	{ CSUBVV, "csubvv" },

	// mul
	{ CMULII, "cmulii" },
	{ CMULID, "cmulid" },
	{ CMULDI, "cmuldi" },
	{ CMULDD, "cmuldd" },
	{ CMULVV, "cmulvv" },

	// div
	{ CDIVII, "cdivii" },
	{ CDIVID, "cdivid" },
	{ CDIVDI, "cdivdi" },
	{ CDIVDD, "cdivdd" },

	// mod
	{ CMODII, "cmodii" },

	// neg
	{ CNEGI, "cnegi" },
	{ CNEGD, "cnegd" },

	// cat
	{ CCATSS, "ccatss" },

	// idx
	{ CIDXJS, "cidxjs" },
	{ CIDXJI, "cidxji" },
	{ CIDXVI, "cidxvi" },

	// like
	{ CLIKESS, "clikess" },

	{ CIN, "cin" },
	{ CALL, "call" },
	{ CANY, "cany" },
	{ CEXISTS, "cexists" },

	// set
	{ CSET, "cset" },
	{ CSET_ORDERED, "cset_ordered" },
	{ CSET_SORT, "cset_sort" },
	{ CSET_ADD, "cset_add" },
	{ CSET_GET, "cset_get" },
	{ CSET_AGG, "cset_agg" },

	// merge
	{ CMERGE, "cmerge" },
	{ CMERGE_RECV, "cmerge_recv" },
	{ CMERGE_RECV_AGG, "cmerge_recv_agg" },

	// counters
	{ CCNTR_INIT, "ccntr_init" },
	{ CCNTR_GTE, "ccntr_gte" },
	{ CCNTR_LTE, "ccntr_lte" },

	// table cursor
	{ CCURSOR_OPEN, "ccursor_open" },
	{ CCURSOR_PREPARE, "ccursor_prepare" },
	{ CCURSOR_CLOSE, "ccursor_close" },
	{ CCURSOR_NEXT, "ccursor_next" },
	{ CCURSOR_READB, "ccursor_readb" },
	{ CCURSOR_READI8, "ccursor_readi8" },
	{ CCURSOR_READI16, "ccursor_readi16" },
	{ CCURSOR_READI32, "ccursor_readi32" },
	{ CCURSOR_READI64, "ccursor_readi64" },
	{ CCURSOR_READF, "ccursor_readf" },
	{ CCURSOR_READD, "ccursor_readd" },
	{ CCURSOR_READT, "ccursor_readt" },
	{ CCURSOR_READL, "ccursor_readl" },
	{ CCURSOR_READS, "ccursor_reads" },
	{ CCURSOR_READJ, "ccursor_readj" },
	{ CCURSOR_READV, "ccursor_readv" },

	// set cursor
	{ CCURSOR_SET_OPEN, "ccursor_set_open" },
	{ CCURSOR_SET_CLOSE, "ccursor_set_close" },
	{ CCURSOR_SET_NEXT, "ccursor_set_next" },
	{ CCURSOR_SET_READ, "ccursor_set_read" },

	// merge cursor
	{ CCURSOR_MERGE_OPEN, "ccursor_merge_open" },
	{ CCURSOR_MERGE_CLOSE, "ccursor_merge_close" },
	{ CCURSOR_MERGE_NEXT, "ccursor_merge_next" },
	{ CCURSOR_MERGE_READ, "ccursor_merge_read" },

	// aggs
	{ CAGG, "cagg" },
	{ CCOUNT, "ccount" },
	{ CAVGI, "cavgi" },
	{ CAVGD, "cavgd" },

	// functions
	{ CCALL, "ccall" },

	// dml
	{ CINSERT, "cinsert" },
	{ CUPSERT, "cupsert" },
	{ CUPDATE, "cupdate" },
	{ CDELETE, "cdelete" },

	// result
	{ CSEND, "csend" },
	{ CSEND_HASH, "csend_hash" },
	{ CSEND_GENERATED, "csend_generated" },
	{ CSEND_FIRST, "csend_first" },
	{ CSEND_ALL, "csend_all" },
	{ CRECV, "crecv" },
	{ CRECV_TO, "crecv_to" },

	// result
	{ CRESULT, "cresult" },
	{ CBODY, "cbody" },
	{ CCTE_SET, "ccte_set" },
	{ CCTE_GET, "ccte_get" },

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
		case CCURSOR_OPEN:
		{
			Str schema;
			Str table;
			Str index;
			uint8_t* ref = code_data_at(data, op->b);
			data_read_string(&ref, &schema);
			data_read_string(&ref, &table);
			data_read_string(&ref, &index);
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
		case CCURSOR_PREPARE:
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
		default:
			op_write(output, op, true, true, true, NULL);
			break;
		}
		encode_raw(buf, (char*)output->start, buf_size(output));
		op++;
	}

	encode_obj_end(buf);
}
