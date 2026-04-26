
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
#include <amelie_row.h>

void
row_encode(Row* self, Columns* columns, Timezone* tz, Buf* buf)
{
	encode_obj(buf);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// name
		encode_string(buf, &column->name);

		// null
		uint8_t* pos = row_column(self, column);
		if (! pos)
		{
			encode_null(buf);
			continue;
		}

		switch (column->type) {
		case TYPE_NULL:
			encode_null(buf);
			break;
		case TYPE_BOOL:
			encode_bool(buf, *(int8_t*)pos);
			break;
		case TYPE_INT:
			switch (column->type_size) {
			case sizeof(int8_t):
				encode_int(buf, *(int8_t*)pos);
				break;
			case sizeof(int16_t):
				encode_int(buf, *(int16_t*)pos);
				break;
			case sizeof(int32_t):
				encode_int(buf, *(int32_t*)pos);
				break;
			case sizeof(int64_t):
				encode_int(buf, *(int64_t*)pos);
				break;
			default:
				abort();
				break;
			}
			break;
		case TYPE_DOUBLE:
			if (column->type_size == sizeof(float))
				encode_real(buf, *(float*)pos);
			else
				encode_real(buf, *(double*)pos);
			break;
		case TYPE_STRING:
		case TYPE_JSON:
			buf_write(buf, pos, data_sizeof(pos));
			break;
		case TYPE_DATE:
			encode_date(buf, *(int32_t*)pos);
			break;
		case TYPE_TIMESTAMP:
			encode_timestamp(buf, tz, *(int64_t*)pos);
			break;
		case TYPE_INTERVAL:
			encode_interval(buf, (Interval*)pos);
			break;
		case TYPE_VECTOR:
			encode_vector(buf, (Vector*)pos);
			break;
		case TYPE_UUID:
			encode_uuid(buf, (Uuid*)pos);
			break;
		default:
			abort();
			break;
		}
	}
	encode_obj_end(buf);
}
