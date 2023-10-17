
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
#include <monotone_request.h>

void
code_save(Code* self, CodeData* data, Buf* buf)
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
	encode_buf(buf, &data->data);
}

void
code_load(Code* self, CodeData* data, uint8_t** pos)
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
	uint8_t* ptr = *pos;
	data_skip(pos);
	buf_write(&data->data, ptr, *pos - ptr);
}
