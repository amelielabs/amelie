#pragma once

//
// monotone
//
// SQL OLTP database
//

always_inline static inline Ast*
parser_next(Parser* self)
{
	return lex_next(&self->lex);
}

always_inline static inline Ast*
parser_if(Parser* self, int id)
{
	return lex_if(&self->lex, id);
}

always_inline static inline void
parser_push(Parser* self, Ast* ast)
{
	lex_push(&self->lex, ast);
}
