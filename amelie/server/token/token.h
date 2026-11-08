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
	Str      db;
	uint64_t expire;
	uint64_t seq_db;
	uint64_t seq_secret;
};

static inline void
token_init(Token* self)
{
	self->expire     = UINT64_MAX;
	self->seq_db     = 0;
	self->seq_secret = 0;
	str_init(&self->token);
	str_init(&self->db);
}

static inline void
token_free(Token* self)
{
	str_free(&self->token);
	str_free(&self->db);
}

static inline void
token_reset(Token* self)
{
	self->expire     = UINT64_MAX;
	self->seq_db     = 0;
	self->seq_secret = 0;
	str_free(&self->token);
	str_free(&self->db);
}

static inline bool
token_empty(Token* self)
{
	return str_empty(&self->db);
}

static inline void
token_set(Token*   self, Str* token, Str* db,
          uint64_t seq_db,
          uint64_t seq_secret,
          uint64_t expire)
{
	self->expire     = expire;
	self->seq_db     = seq_db;
	self->seq_secret = seq_secret;
	str_free(&self->db);
	str_free(&self->token);
	str_copy(&self->token, token);
	str_copy(&self->db, db);
}
