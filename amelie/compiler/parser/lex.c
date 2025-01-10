
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
lex_init(Lex* self, Keyword** keywords)
{
	self->pos             = NULL;
	self->end             = NULL;
	self->start           = NULL;
	self->keywords        = keywords;
	self->keywords_enable = true;
	self->backlog         = NULL;
}

void
lex_reset(Lex* self)
{
	self->pos             = NULL;
	self->end             = NULL;
	self->start           = NULL;
	self->keywords_enable = true;
	self->backlog         = NULL;
}

void
lex_set_keywords(Lex* self, bool keywords)
{
	self->keywords_enable = keywords;
}

void
lex_error(Lex* self, Ast* ast, const char* error)
{
	Str before;
	str_set(&before, self->start, ast->pos_start);
	Str tk;
	str_set(&tk, self->start + ast->pos_start, ast->pos_end - ast->pos_start);

	if (ast->id == KEOF)
		error("%.*s ⟵ %s", str_size(&before), str_of(&before), error);

	error("%.*s❰%.*s❱ ⟵ %s", str_size(&before), str_of(&before),
	      str_size(&tk), str_of(&tk), error);
}

void
lex_error_expect(Lex* self, Ast* ast, int id)
{
	char name[64];
	if (id < 128)
	{
		snprintf(name, sizeof(name), "%c expected", id);
	} else
	if (id > KKEYWORD)
	{
		auto keyword = &keywords[id - KKEYWORD - 1];
		assert(keyword->name_size < (int)sizeof(name));
		for (auto i = 0; i < keyword->name_size; i++)
			name[i] = toupper(keyword->name[i]);
		snprintf(name + keyword->name_size, sizeof(name) - keyword->name_size,
		         " expected");
	} else
	{
		char* ref = "<unknown>";
		switch (id) {
		// consts
		case KREAL:
			ref = "float";
			break;
		case KINT:
			ref = "int";
			break;
		case KSTRING:
			ref = "string";
			break;
		case KARGID:
			ref = "argument";
			break;
		// lexer operations
		case KSHL:
			ref = "<<";
			break;
		case KSHR:
			ref = ">>";
			break;
		case KLTE:
			ref = "<=";
			break;
		case KGTE:
			ref = ">=";
			break;
		case KNEQU:
			ref = "<>";
			break;
		case KCAT:
			ref = "||";
			break;
		case KARROW:
			ref = "->";
			break;
		case KMETHOD:
			ref = "::";
			break;
		// name/path
		case KNAME:
			ref = "name";
			break;
		case KNAME_COMPOUND:
			ref = "name.path";
			break;
		case KNAME_COMPOUND_STAR:
			ref = "target.*";
			break;
		case KSTAR:
			ref = "*";
			break;
		case KEOF:
			ref = "<eof>";
			break;
		}
		snprintf(name, sizeof(name), "%s expected", ref);
	}
	lex_error(self, ast, name);
}

hot static inline Keyword*
lex_keyword_match(Lex* self, Str* name)
{
	if (self->keywords == NULL)
		return NULL;
	auto ref = tolower(name->pos[0]);
	auto current = self->keywords[ref - 'a'];
	for (; current && current->name; current++)
	{
		if (*current->name != ref)
			break;
		if (str_is_case(name, current->name, current->name_size))
			return current;
	}
	return NULL;
}

void
lex_start(Lex* self, Str* text)
{
	self->start   = str_of(text);
	self->pos     = self->start;
	self->end     = self->start + str_size(text);
	self->backlog = NULL;
}

void
lex_push(Lex* self, Ast* ast)
{
	ast->next = NULL;
	ast->prev = self->backlog;
	self->backlog = ast;
}

hot always_inline static inline Ast*
lex_return(Lex* self, Ast* ast, int id, char* start)
{
	ast->id        = id;
	ast->pos_start = start - self->start;
	ast->pos_end   = self->pos - self->start;
	return ast;
}

hot always_inline static inline void
lex_return_error(Lex* self, Ast* ast, char* start, char* msg)
{
	ast->pos_start = start - self->start;
	ast->pos_end = self->pos - self->start;
	lex_error(self, ast, msg);
}

hot Ast*
lex_next(Lex* self)
{
	if (self->backlog)
	{
		auto last = self->backlog;
		self->backlog = last->prev;
		last->prev = NULL;
		return last;
	}
	Ast* ast = ast_allocate(KEOF, sizeof(Ast));

	// skip white spaces and comments
	for (;;)
	{
		while (self->pos < self->end && isspace(*self->pos))
			self->pos++;

		// eof
		if (unlikely(self->pos == self->end))
			return lex_return(self, ast, KEOF, self->end);

		// --
		if (*self->pos == '-')
		{
			if ((self->end - self->pos) >= 2 && (self->pos[1] == '-'))
			{
				while (self->pos < self->end && *self->pos != '\n')
					self->pos++;
				continue;
			}
		}
		break;
	}
	auto start = self->pos;

	// argument
	if (*self->pos == '$')
	{
		self->pos++;
		if (unlikely(self->pos == self->end || !isdigit(*self->pos)))
			lex_return_error(self, ast, start, "bad argument definition");

		// $<int>
		while (self->pos < self->end && isdigit(*self->pos)) {
			ast->integer = (ast->integer * 10) + *self->pos - '0';
			self->pos++;
		}
		return lex_return(self, ast, KARGID, start);
	}

	// -
	auto minus = *self->pos == '-';
	if (minus)
		self->pos++;

	// integer or float
	if (isdigit(*self->pos))
	{
		while (self->pos < self->end)
		{
			// float
			if (*self->pos == '.' ||
			    *self->pos == 'e' ||
			    *self->pos == 'E')
			{
				self->pos = start;
				goto reread_as_float;
			}
			if (! isdigit(*self->pos))
				break;
			ast->integer = (ast->integer * 10) + *self->pos - '0';
			self->pos++;
		}
		if (minus)
			ast->integer = -ast->integer;
		return lex_return(self, ast, KINT, start);

reread_as_float:
		if (minus)
			self->pos--;
		errno = 0;
		char* end = NULL;
		errno = 0;
		ast->real = strtod(start, &end);
		if (errno == ERANGE)
			lex_return_error(self, ast, start, "bad float number token");
		self->pos = end;
		return lex_return(self, ast, KREAL, start);
	}

	// -
	if (minus)
		self->pos--;

	// symbols
	if (*self->pos != '\"' &&
	    *self->pos != '\'' && *self->pos != '_' && ispunct(*self->pos))
	{
		char symbol = *self->pos;
		self->pos++;
		if (unlikely(self->pos == self->end))
			goto symbol;
		char symbol_next = *self->pos;
		// ::
		if (symbol == ':' && symbol_next == ':') {
			self->pos++;
			return lex_return(self, ast, KMETHOD, start);
		}
		// ||
		if (symbol == '|' && symbol_next == '|') {
			self->pos++;
			return lex_return(self, ast, KCAT, start);
		}
		// ->
		if (symbol == '-' && symbol_next == '>') {
			self->pos++;
			return lex_return(self, ast, KARROW, start);
		}
		// <>
		if (symbol == '<' && symbol_next == '>') {
			self->pos++;
			return lex_return(self, ast, KNEQU, start);
		}
		// >=
		if (symbol == '>' && symbol_next == '=') {
			self->pos++;
			return lex_return(self, ast, KGTE, start);
		}
		// <=
		if (symbol == '<' && symbol_next == '=') {
			self->pos++;
			return lex_return(self, ast, KLTE, start);
		}
		// <<
		if (symbol == '<' && symbol_next == '<') {
			self->pos++;
			return lex_return(self, ast, KSHL, start);
		}
		// >>
		if (symbol == '>' && symbol_next == '>') {
			self->pos++;
			return lex_return(self, ast, KSHR, start);
		}
symbol:;
		ast->integer = 0;
		return lex_return(self, ast, symbol, start);
	}

	// keyword or name
	if (isalpha(*self->pos) || *self->pos == '_')
	{
		char* compound_prev = NULL;
		while (self->pos < self->end &&
		       (*self->pos == '_' ||
	 	        *self->pos == '.' ||
	 	        *self->pos == '$' ||
	 	         isalpha(*self->pos) ||
		         isdigit(*self->pos)))
		{
			if (*self->pos == '.') {
				if (compound_prev == (self->pos - 1))
					lex_error(self, ast, "bad compound name token");
				compound_prev = self->pos;
			}
			self->pos++;
		}
		str_set(&ast->string, start, self->pos - start);

		if (compound_prev)
	   	{
			if (unlikely(*(self->pos - 1) == '.'))
			{
				// path.*
				if (self->pos != self->end && *self->pos == '*')
				{
					self->pos++;
					ast->string.end++;
					return lex_return(self, ast, KNAME_COMPOUND_STAR, start);
				}
				lex_error(self, ast, "bad compound name token");
			}
			return lex_return(self, ast, KNAME_COMPOUND, start);
		}

		if (*ast->string.pos != '_' && self->keywords_enable)
		{
			auto keyword = lex_keyword_match(self, &ast->string);
			if (keyword) 
				return lex_return(self, ast, keyword->id, start);
		}
		return lex_return(self, ast, KNAME, start);
	}

	// string
	if (*self->pos == '\"' || *self->pos == '\'')
	{
		char end_char = '"';
		if (*self->pos == '\'')
			end_char = '\'';
		self->pos++;

		auto string = self->pos;
		bool slash  = false;
		bool escape = false;
		while (self->pos < self->end)
		{
			if (*self->pos == end_char) {
				if (slash) {
					slash = false;
					self->pos++;
					continue;
				}
				break;
			}
			if (*self->pos == '\\') {
				slash = !slash;
				escape = true;
			} else {
				slash = false;
			}
			self->pos++;
		}
		if (unlikely(self->pos == self->end))
			lex_return_error(self, ast, string, "unterminated string");

		str_set(&ast->string, string, self->pos - string);
		ast->string_escape = escape;
		self->pos++;
		return lex_return(self, ast, KSTRING, start);
	}

	// error
	lex_return_error(self, ast, start, "bad token");
	return NULL;
}

hot Ast*
lex_if(Lex* self, int id)
{
	auto ast = lex_next(self);
	if (ast->id == id)
		return ast;
	lex_push(self, ast);
	return NULL;
}
