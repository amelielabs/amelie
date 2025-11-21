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
parse_type(Lex* self, int* type_size)
{
	auto ast = lex_next_shadow(self);
	if (ast->id != KNAME)
		lex_error(self, ast, "unrecognized data type");

	auto type = type_read(&ast->string, type_size);
	if (type == -1)
		lex_error(self, ast, "unrecognized data type");

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
	return type;
}
