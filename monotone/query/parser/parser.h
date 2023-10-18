#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Parser Parser;

enum
{
	EXPLAIN_NONE    = 0,
	EXPLAIN         = 1,
	EXPLAIN_PROFILE = 2
};

struct Parser
{
	bool in_transaction;
	bool complete;
	int  explain;
	List stmts;
	int  stmts_count;
	Lex  lex;
	Db*  db;
};

static inline void
parser_init(Parser* self, Db* db)
{
	self->in_transaction = false;
	self->complete       = false;
	self->explain        = EXPLAIN_NONE;
	self->stmts_count    = 0;
	self->db             = db;
	list_init(&self->stmts);
	lex_init(&self->lex, keywords);
}

static inline void
parser_reset(Parser* self)
{
	self->in_transaction = false;
	self->complete       = false;
	self->explain        = EXPLAIN_NONE;
	self->stmts_count    = 0;
	// todo: free stmts
	list_init(&self->stmts);
	lex_reset(&self->lex);
}
