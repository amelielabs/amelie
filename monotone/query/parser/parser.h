#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Parser Parser;

struct Parser
{

	bool in_transaction;
	bool complete;
	List stmts;
	int  stmts_count;
	Lex  lex;
	Db*  db;
};

static inline void
parser_init(Parser* self, Db* db)
{
	self->db = db;
	self->in_transaction = false;
	self->complete = false;
	self->stmts_count = 0;
	list_init(&self->stmts);
	lex_init(&self->lex, keywords);
}

static inline void
parser_reset(Parser* self)
{
	self->in_transaction = false;
	self->complete = false;
	// todo: free stmts
	self->stmts_count = 0;
	list_init(&self->stmts);
	lex_reset(&self->lex);
}
