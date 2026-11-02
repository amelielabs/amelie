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
	Str      user;
	uint64_t expire;
	uint64_t seq_user;
	uint64_t seq_secret;
};

static inline void
token_init(Token* self)
{
	self->expire     = UINT64_MAX;
	self->seq_user   = 0;
	self->seq_secret = 0;
	str_init(&self->token);
	str_init(&self->user);
}

static inline void
token_free(Token* self)
{
	str_free(&self->token);
	str_free(&self->user);
}

static inline void
token_reset(Token* self)
{
	self->expire     = UINT64_MAX;
	self->seq_user   = 0;
	self->seq_secret = 0;
	str_free(&self->token);
	str_free(&self->user);
}

static inline bool
token_empty(Token* self)
{
	return str_empty(&self->user);
}

static inline void
token_set(Token*   self, Str* token, Str* user,
          uint64_t seq_user,
          uint64_t seq_secret,
          uint64_t expire)
{
	self->expire     = expire;
	self->seq_user   = seq_user;
	self->seq_secret = seq_secret;
	str_free(&self->user);
	str_free(&self->token);
	str_copy(&self->token, token);
	str_copy(&self->user, user);
}
