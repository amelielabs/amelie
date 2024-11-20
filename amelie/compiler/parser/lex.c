
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
lex_init(Lex* self, Keyword** keywords)
{
	self->pos             = NULL;
	self->end             = NULL;
	self->keywords        = keywords;
	self->keywords_enable = true;
	self->backlog         = NULL;
	self->prefix          = NULL;
}

void
lex_reset(Lex* self)
{
	self->pos             = NULL;
	self->end             = NULL;
	self->keywords_enable = true;
	self->backlog         = NULL;
	self->prefix          = NULL;
}

void
lex_set_keywords(Lex* self, bool keywords)
{
	self->keywords_enable = keywords;
}

void
lex_set_prefix(Lex* self, const char* prefix)
{
	self->prefix = prefix;
}

static inline void
lex_error(Lex* self, const char* error)
{
	if (self->prefix)
		error("%s: %s", self->prefix, error);
	else
		error("%s", error);
}

static inline Keyword*
lex_keyword_match(Lex* self, Str* name)
{
	if (self->keywords == NULL)
		return NULL;
	auto current = self->keywords[tolower(name->pos[0]) - 'a'];
	for (; current->name; current++)
		if (str_is_case(name, current->name, current->name_size))
			return current;
	return NULL;
}

void
lex_start(Lex* self, Str* text)
{
	self->pos     = str_of(text);
	self->end     = str_of(text) + str_size(text);
	self->backlog = NULL;
	self->prefix  = NULL;
}

void
lex_push(Lex* self, Ast* ast)
{
	ast->next = NULL;
	ast->prev = self->backlog;
	self->backlog = ast;
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
	ast->pos = self->pos;

	// skip white spaces and comments
	for (;;)
	{
		while (self->pos < self->end && isspace(*self->pos))
			self->pos++;
		// eof
		if (unlikely(self->pos == self->end))
			return ast;
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

	// argument
	if (*self->pos == '$')
	{
		self->pos++;
		if (unlikely(self->pos == self->end))
			lex_error(self, "bad argument definition");
		if (isdigit(*self->pos))
		{
			// $<int>
			ast->id = KARGID;
			while (self->pos < self->end && isdigit(*self->pos)) {
				ast->integer = (ast->integer * 10) + *self->pos - '0';
				self->pos++;
			}
		} else
		{
			lex_error(self, "bad argument definition");
		}
		return ast;
	}

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
			ast->id = KMETHOD;
			return ast;
		}
		// :=
		if (symbol == ':' && symbol_next == '=') {
			self->pos++;
			ast->id = KASSIGN;
			return ast;
		}
		// <>
		if (symbol == '<' && symbol_next == '>') {
			self->pos++;
			ast->id = KNEQU;
			return ast;
		}
		// >>
		if (symbol == '>' && symbol_next == '>') {
			self->pos++;
			ast->id = KSHR;
			return ast;
		}
		// >=
		if (symbol == '>' && symbol_next == '=') {
			self->pos++;
			ast->id = KGTE;
			return ast;
		}
		// <=
		if (symbol == '<' && symbol_next == '=') {
			self->pos++;
			ast->id = KLTE;
			return ast;
		}
		// <<
		if (symbol == '<' && symbol_next == '<') {
			self->pos++;
			ast->id = KSHL;
			return ast;
		}
		// ||
		if (symbol == '|' && symbol_next == '|') {
			self->pos++;
			ast->id = KCAT;
			return ast;
		}
symbol:;
		ast->id = symbol;
		ast->integer = 0;
		return ast;
	}

	// integer or float
	if (isdigit(*self->pos))
	{
		ast->id = KINT;
		auto start = self->pos;
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
		return ast;

reread_as_float:
		errno = 0;
		ast->id = KREAL;
		char* end = NULL;
		errno = 0;
		ast->real = strtod(start, &end);
		if (errno == ERANGE)
			lex_error(self, "bad float number token");
		self->pos = end;
		return ast;
	}

	// keyword or name
	if (isalpha(*self->pos) || *self->pos == '_')
	{
		auto start = self->pos;
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
					lex_error(self, "bad compound name token");
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
					ast->id = KNAME_COMPOUND_STAR;
					return ast;
				}
				lex_error(self, "bad compound name token");
			}
			ast->id = KNAME_COMPOUND;
			return ast;
		}

		ast->id = KNAME;
		if (*ast->string.pos != '_' && self->keywords_enable)
		{
			auto keyword = lex_keyword_match(self, &ast->string);
			if (keyword) 
				ast->id = keyword->id;
		}
		return ast;
	}

	// string
	if (*self->pos == '\"' || *self->pos == '\'')
	{
		char end_char = '"';
		if (*self->pos == '\'')
			end_char = '\'';
		self->pos++;

		auto start  = self->pos;
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
			if (*self->pos == '\n')
				lex_error(self, "unterminated string");
			if (*self->pos == '\\') {
				slash = !slash;
				escape = true;
			} else {
				slash = false;
			}
			self->pos++;
		}
		if (unlikely(self->pos == self->end))
			lex_error(self, "unterminated string");

		ast->id = KSTRING;
		ast->string_escape = escape;
		str_set(&ast->string, start, self->pos - start);
		self->pos++;
		return ast;
	}

	// error
	lex_error(self, "bad token");
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
