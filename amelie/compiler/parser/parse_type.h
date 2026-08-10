#pragma once

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

static inline int
parse_type(Lex* self)
{
	auto ast = lex_next_shadow(self);
	if (ast->id != KNAME)
		lex_error(self, ast, "unrecognized data type");

	int  type_size;
	auto type = type_read(&ast->string, &type_size);
	if (type == -1)
		lex_error(self, ast, "unrecognized data type");

	return type;
}

static inline void
parse_type_column(Lex* self, Local* local, Column* column)
{
	auto ast = lex_next_shadow(self);
	if (ast->id != KNAME)
		lex_error(self, ast, "unrecognized data type");

	int  size_flat = 0;
	int  size;
	auto type = type_read(&ast->string, &size);
	if (type == -1)
		lex_error(self, ast, "unrecognized data type");

	if (type == TYPE_DECIMAL)
	{
		// DECIMAL [(p, s)]
		if (lex_if(self, '('))
		{
			auto p = lex_expect(self, KINT);
			lex_expect(self, ',');
			auto s = lex_expect(self, KINT);
			lex_expect(self, ')');

			// validate values
			if (p->integer < 1 || p->integer > DECIMAL_MAX_PRECISION)
				lex_error(self, p, "supported decimal precision is 1-15");

			if (s->integer < 0 || s->integer > DECIMAL_MAX_SCALE)
				lex_error(self, s, "supported decimal scale is 0-15");

			if (s->integer > p->integer)
				lex_error(self, s, "invalid decimal scale");

			constraints_set_decimal(&column->constraints, p->integer, s->integer);
		}
	} else
	if (type == TYPE_VECTOR)
	{
		lex_expect(self, '(');
		auto ast = lex_expect(self, KINT);
		if (ast->integer < 1)
			lex_error(self, ast, "invalid vector dimension");
		size_flat = ast->integer * sizeof(float);
		lex_expect(self, ')');

		// check vector limit
		if (! local_limit(local, LIMIT_VECTOR, ast->integer))
			lex_error(self, ast, "vector size limit");
	} else
	if (str_is_case(&ast->string, "char", 4))
	{
		// CHAR [(size)]
		if (lex_if(self, '('))
		{
			lex_expect(self, KINT);
			lex_expect(self, ')');
		}
	} else
	if (str_is_case(&ast->string, "varchar", 7))
	{
		// VARCHAR [(size)]
		if (lex_if(self, '('))
		{
			lex_expect(self, KINT);
			lex_expect(self, ')');
		}
	}

	column_set_type(column, type, size);
	column_set_size_flat(column, size_flat);
}
