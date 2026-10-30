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

typedef struct Token Token;

struct Token
{
	Str     token;
	int64_t expire;
	void*   user;
	bool    trusted;
};

static inline void
token_init(Token* self)
{
	self->user    = NULL;
	self->expire  = -1;
	self->trusted = false;
	str_init(&self->token);
}

static inline void
token_free(Token* self)
{
	str_free(&self->token);
}

static inline void
token_set_trusted(Token* self, bool value)
{
	self->trusted = value;
}

static inline void
token_set(Token* self, Str* token, int64_t expire, void* user)
{
	self->expire = expire;
	self->user   = user;

	str_free(&self->token);
	if (! str_empty(token))
		str_copy(&self->token, token);
}
