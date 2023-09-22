#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Request Request;

struct Request
{
	bool       ro;
	Condition* on_commit; // set after ready for !ro
	// code
	// transaction
	Channel*   portal;
};

static inline void
request_init(Request* self)
{
	self->ro        = false;
	self->on_commit = NULL;
	self->portal    = NULL;
}
