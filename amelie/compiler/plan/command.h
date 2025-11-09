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

typedef struct Command     Command;
typedef struct CommandScan CommandScan;

typedef enum
{
	COMMAND_EXPR,
	COMMAND_EXPR_SET,
	COMMAND_SCAN,
	COMMAND_SCAN_ORDERED,
	COMMAND_SCAN_AGGS,
	COMMAND_SCAN_AGGS_ORDERED,
	COMMAND_PIPE,
	COMMAND_SORT,
	COMMAND_UNION,
	COMMAND_UNION_AGGS,
	COMMAND_RECV,
	COMMAND_RECV_AGGS
} CommandId;

struct Command
{
	CommandId id;
	List      link;
};

struct CommandScan
{
	Command cmd;
	Ast*    expr_limit;
	Ast*    expr_offset;
	Ast*    expr_where;
	From*   from;
};

static inline void*
command_allocate(CommandId id, int size)
{
	assert(size >= (int)sizeof(Command));
	auto self = (Command*)palloc(size);
	memset(self, 0, size);
	self->id = id;
	list_init(&self->link);
	return self;
}
