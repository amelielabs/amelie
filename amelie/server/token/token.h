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
	Str      token;
	uint64_t expire;
	void*    user;
};

static inline void
token_init(Token* self)
{
	self->user   = NULL;
	self->expire = UINT64_MAX;
	str_init(&self->token);
}

static inline void
token_free(Token* self)
{
	str_free(&self->token);
}

static inline void
token_reset(Token* self)
{
	self->user   = NULL;
	self->expire = UINT64_MAX;
	str_free(&self->token);
}

static inline bool
token_empty(Token* self)
{
	return !self->user;
}

static inline void
token_set(Token* self, Str* token, uint64_t expire, void* user)
{
	self->expire = expire;
	self->user   = user;
	str_free(&self->token);
	if (! str_empty(token))
		str_copy(&self->token, token);
}
