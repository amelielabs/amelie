
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

static inline void
parser_value_add(Parser* self)
{
	auto stack = &self->query.stack;
	if (ast_head(stack))
		ast_head(stack)->integer++;
}

hot static inline AstValue*
parser_value(Parser* self, AstRow* row)
{
	auto stack = &self->query.stack;
	auto stack_start = ast_head(stack);

	auto value = ast_value_allocate(0);
	Ast* last  = NULL;
	bool done  = false;
	while (! done)
	{
		auto ast = parser_next(self);
		switch (ast->id) {
		case '[':
			parser_value_add(self);
			ast_push(stack, ast);
			break;
		case ']':
		{
			auto head = ast_head(stack);
			if (!head || head->id != '[')
				error("expected ']'");
			row->data_size += data_size_array(head->integer);
			ast_pop(stack);
			continue;
		}
		case '{':
			parser_value_add(self);
			ast_push(stack, ast);
			break;
		case '}':
		{
			auto head = ast_head(stack);
			if (!head || head->id != '{')
				error("expected '}'");
			row->data_size += data_size_map(head->integer);
			ast_pop(stack);
			continue;
		}
		case ':':
		case ',':
			// todo
			continue;
		case KSTRING:
			parser_value_add(self);
			row->data_size += data_size_string(str_size(&ast->string));
			break;
		case KNULL:
			parser_value_add(self);
			row->data_size += data_size_null();
			break;
		case KTRUE:
		case KFALSE:
			parser_value_add(self);
			row->data_size += data_size_bool();
			break;
		case KINT:
			parser_value_add(self);
			row->data_size += data_size_integer(ast->integer);
			break;
		case KFLOAT:
			parser_value_add(self);
			row->data_size += data_size_float();
			break;
		default:
			done = true;
			parser_push(self, ast);
			continue;
		}

		if (value->expr == NULL)
			value->expr = ast;
		else
			last->next = ast;
		last = ast;
	}

	if (stack_start != ast_head(stack))
		error("bad row value");

	return value;
}

hot AstRow*
parser_row(Parser* self, Table* table) 
{
	// (value, ...)

	// (
	if (! parser_if(self, '('))
		error("<(> expected");

	auto row = ast_row_allocate(0);

	auto column = table->config->schema.column;
	Ast* last   = NULL;
	for (; column; column = column->next)
	{
		auto value = parser_value(self, row);
		if (unlikely(value->expr == NULL))
			error("bad row value");

		// validate key and hash
		if (column_is_key(column))
		{
			uint8_t* key;
			int      key_size;

			if (column->type == TYPE_STRING)
			{
				if (value->expr->id != KSTRING)
					error("key column <%.*s>: expected string type", str_size(&column->name),
					      str_of(&column->name));

				key = str_u8(&value->expr->string);
				key_size = str_size(&value->expr->string);
			} else
			{
				if (value->expr->id != KINT)
					error("key column <%.*s>: expected int type", str_size(&column->name),
					      str_of(&column->name));

				key = (uint8_t*)&value->expr->integer;
				key_size = sizeof(value->expr->integer);
			}

			row->hash = hash_murmur3_32(key, key_size, row->hash);
		}

		if (row->list == NULL)
			row->list = &value->ast;
		else
			last->next = &value->ast;
		last = &value->ast;
		row->list_count++;

		// ,
		if (parser_if(self, ','))
			continue;

		// )
		if (! parser_if(self, ')'))
			error("expected ')'");
	}

	return row;
}
