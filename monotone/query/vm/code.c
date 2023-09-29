
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>

void
code_save(Code* self, Buf* buf)
{
	// map
	encode_map(buf, 2);

	// code 
	encode_raw(buf, "code", 4);
	encode_array(buf, code_count(self));

	auto op  = (Op*)self->code.start;
	auto end = (Op*)self->code.position;
	for (; op < end; op++)
	{
		encode_array(buf, 5);
		encode_integer(buf, op->op);
		encode_integer(buf, op->a);
		encode_integer(buf, op->b);
		encode_integer(buf, op->c);
		encode_integer(buf, op->d);
	}

	// data
	encode_raw(buf, "data", 4);
	if (buf_size(&self->data) > 0)
		buf_write(buf, self->data.start, buf_size(&self->data));
	else
		encode_null(buf);
}

void
code_load(Code* self, uint8_t** pos)
{
	// map
	int count;
	data_read_map(pos, &count);

	// code
	data_skip(pos);
	data_read_array(pos, &count);
	for (int i = 0; i < count; i++)
	{
		// array
		int _unused = 0;
		data_read_array(pos, &_unused);

		int64_t op, a, b, c, d;
		data_read_integer(pos, &op);
		data_read_integer(pos, &a);
		data_read_integer(pos, &b);
		data_read_integer(pos, &c);
		data_read_integer(pos, &d);

		code_add(self, op, a, b, c, d);
	}

	// data
	data_skip(pos);
	uint8_t* data = *pos;
	data_skip(pos);
	if (! data_is_null(data))
		buf_write(&self->data, data, *pos - data);
}

void
code_dump(Code* self, Buf* output)
{
	buf_printf(output, "%s", "\n");
	buf_printf(output, "%s", "bytecode\n");
	buf_printf(output, "%s", "--------\n");

	auto op  = (Op*)self->code.start;
	auto end = (Op*)self->code.position;

	int i = 0;
	while (op < end)
	{
		buf_printf(output,
		           "%2d  %18s   %6" PRIi64 " %4" PRIi64 " %4" PRIi64 " %12d      ",
		           i,
		           ops[op->op].name, op->a, op->b, op->c,
		           op->count);

		switch (op->op) {
		case CSTRING:
		{
			Str str;
			code_read_string(self, op->b, &str);
			buf_printf(output, "# %.*s", str_size(&str), str_of(&str));
			break;
		}
		case CFLOAT:
		{
			float value;
			value = code_read_fp(self, op->b);
			buf_printf(output, "# %f", value);
			break;
		}
		case CINSERT:
		{
			Str str;
			code_read_string(self, op->b, &str);
			buf_printf(output, "# %.*s", str_size(&str), str_of(&str));
			break;
		}
		case CDELETE:
			break;
		case CCURSOR_OPEN:
		{
			Str str;
			code_read_string(self, op->b, &str);
			buf_printf(output, "# %.*s", str_size(&str), str_of(&str));
			break;
		}
		case CCALL:
		{
			Str str;
			code_read_string(self, op->b, &str);
			buf_printf(output, "# %.*s", str_size(&str), str_of(&str));
			break;
		}
		}
		buf_printf(output, "\n");

		op++;
		i++;
	}
}
