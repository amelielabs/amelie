#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

hot static inline int
emit_row(CodeData* data, AstRow* row, int* data_size)
{
	int start = code_data_pos(data);
	auto buf = &data->data;

	// []
	encode_array(buf);
	auto value = row->list;
	while (value)
	{
		auto expr = ast_value_of(value)->expr;
		while (expr)
		{
			switch (expr->id) {
			case '[':
				encode_array(buf);
				break;
			case ']':
				encode_array_end(buf);
				break;
			case '{':
				encode_map(buf);
				break;
			case '}':
				encode_map_end(buf);
				break;
			case KSTRING:
				encode_string(buf, &expr->string);
				break;
			case KNULL:
				encode_null(buf);
				break;
			case KTRUE:
			case KFALSE:
				encode_bool(buf, expr->id == KTRUE);
				break;
			case KINT:
				encode_integer(buf, expr->integer);
				break;
			case KREAL:
				encode_real(buf, expr->real);
				break;
			default:
				abort();
				break;
			}

			expr = expr->r;
		}

		value = value->next;
	}
	encode_array_end(buf);

	*data_size = buf_size(buf) - start;
	return start;
}
