
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>

void
lex_init(Lex* self, Keyword** keywords, Str* text)
{
	self->pos              = str_of(text);
	self->end              = str_of(text) + str_size(text);
	self->line             = 0;
	self->backlog_count    = 0;
	self->keywords_enabled = true;
	self->keywords         = keywords;
	self->prefix           = NULL;
}

void
lex_enable_keywords(Lex* self, bool on)
{
	self->keywords_enabled = on;
}

void
lex_enable_prefix(Lex* self, const char* prefix)
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
		if (str_strncasecmp(name, current->name, current->name_size))
			return current;
	return NULL;
}

void
lex_push(Lex* self, Token* token)
{
	assert(self->backlog_count < 4);
	self->backlog[self->backlog_count] = *token;
	self->backlog_count++;
}

hot int
lex_next(Lex* self, Token* token)
{
	// try to use backlog
	if (self->backlog_count > 0) {
		*token = self->backlog[self->backlog_count - 1];
		self->backlog_count--;
		return token->type;
	}
	memset(token, 0, sizeof(*token));

	// skip white spaces and comments
	for (;;)
	{
		while (self->pos < self->end && isspace(*self->pos)) {
			if (*self->pos == '\n')
				self->line++;
			self->pos++;
		}
		if (unlikely(self->pos == self->end)) {
			token->type = LEX_EOF;
			token->line = self->line;
			return token->type;
		}
		if (*self->pos != '#')
			break;
		while (self->pos < self->end && *self->pos != '\n')
			self->pos++;
		if (self->pos == self->end) {
			token->line = self->line;
			token->type = LEX_EOF;
			return token->type;
		}
		self->line++;
	}
	token->line = self->line;

	// argument
	if (*self->pos == '$')
	{
		self->pos++;
		if (unlikely(self->pos == self->end))
			lex_error(self, "bad argument definition");
		if (isdigit(*self->pos))
		{
			// $<int>
			token->type = LEX_ARGUMENT;
			token->integer = 0;
			while (self->pos < self->end && isdigit(*self->pos)) {
				token->integer = (token->integer * 10) + *self->pos - '0';
				self->pos++;
			}
		} else
		if (isalpha(*self->pos) || *self->pos == '_')
		{
			// $<name>
			token->type   = LEX_ARGUMENT_NAME;
			token->escape = false;
			auto start = self->pos;
			while (self->pos < self->end && (isalpha(*self->pos) || *self->pos == '_'))
				self->pos++;
			str_set(&token->string, start, self->pos - start);
		} else
		{
			lex_error(self, "bad argument definition");
		}
		return token->type;
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
		// <>
		if (symbol == '<' && symbol_next == '>') {
			self->pos++;
			token->type = LEX_NEQU;
			return token->type;
		}
		// >>
		if (symbol == '>' && symbol_next == '>') {
			self->pos++;
			token->type = LEX_SHR;
			return token->type;
		}
		// >=
		if (symbol == '>' && symbol_next == '=') {
			self->pos++;
			token->type = LEX_GTE;
			return token->type;
		}
		// <=
		if (symbol == '<' && symbol_next == '=') {
			self->pos++;
			token->type = LEX_LTE;
			return token->type;
		}
		// <<
		if (symbol == '<' && symbol_next == '<') {
			self->pos++;
			token->type = LEX_SHL;
			return token->type;
		}
		// ||
		if (symbol == '|' && symbol_next == '|') {
			self->pos++;
			token->type = LEX_CAT;
			return token->type;
		}
symbol:
		token->type    = symbol;
		token->integer = symbol;
		return symbol;
	}

	// integer or float
	if (isdigit(*self->pos))
	{
		token->type = LEX_INT;
		token->integer = 0;
		auto start = self->pos;
		while (self->pos < self->end)
		{
			// float
			if (*self->pos == '.' || 
			    *self->pos == 'e' || 
			    *self->pos == 'E') {
				token->integer = 0;
				self->pos = start;
				goto reread_as_float;
			}
			if (! isdigit(*self->pos))
				break;
			token->integer = (token->integer * 10) + *self->pos - '0';
			self->pos++;
		}
		return token->type;
reread_as_float:
		errno = 0;
		token->type = LEX_FLOAT;
		char* end = NULL;
		token->fp = strtof(start, &end);
		if (errno == ERANGE)
			lex_error(self, "bad float number token");
		self->pos = end;
		return token->type;
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
		token->escape = false;
		str_set(&token->string, start, self->pos - start);

		if (compound_prev) {
			if (unlikely(*(self->pos - 1) == '.'))
			{
				// path.*
				if (self->pos != self->end && *self->pos == '*')
				{
					self->pos++;
					token->string.end++;
					token->type = LEX_NAME_COMPOUND_ASTERISK;
					return token->type;
				}
				lex_error(self, "bad compound name token");
			}
			token->type = LEX_NAME_COMPOUND;
			return token->type;
		}

		token->type = LEX_NAME;
		if (*token->string.pos != '_' && self->keywords_enabled)
		{
			auto keyword = lex_keyword_match(self, &token->string);
			if (keyword) 
				token->type = keyword->id;
		}
		return token->type;
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

		token->type   = LEX_STRING;
		token->escape = escape;
		str_set(&token->string, start, self->pos - start);
		self->pos++;
		return token->type;
	}

	// error
	lex_error(self, "bad token");
	return -1;
}

bool
lex_if(Lex* self, Token* token, int id)
{
	Token unused_token;
	if (token == NULL)
		token = &unused_token;
	int rc;
	rc = lex_next(self, token);
	if (rc != id) {
		lex_push(self, token);
		return false;
	}
	return true;
}
