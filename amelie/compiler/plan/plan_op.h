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

static inline void
plan_add(Plan* self, Command* cmd)
{
	if (self->recv)
		list_append(&self->list_recv, &cmd->link);
	else
		list_append(&self->list, &cmd->link);
}

static inline void
plan_add_expr(Plan* self)
{
	auto cmd = command_allocate(COMMAND_EXPR, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_expr_set(Plan* self)
{
	auto cmd = command_allocate(COMMAND_EXPR_SET, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_scan(Plan* self,
              Ast*  expr_limit,
              Ast*  expr_offset,
              Ast*  expr_where,
              From* from,
              bool  ordered)
{
	auto cmd = (CommandScan*)command_allocate(COMMAND_SCAN, sizeof(CommandScan));
	plan_add(self, &cmd->cmd);
	cmd->expr_limit  = expr_limit;
	cmd->expr_offset = expr_offset;
	cmd->expr_where  = expr_where;
	cmd->from        = from;
	cmd->ordered     = ordered;
}

static inline void
plan_add_scan_aggs(Plan* self, bool ordered, bool child)
{
	auto cmd = (CommandScanAggs*)command_allocate(COMMAND_SCAN_AGGS, sizeof(CommandScanAggs));
	plan_add(self, &cmd->cmd);
	cmd->ordered = ordered;
	cmd->child   = child;
}

static inline void
plan_add_pipe(Plan* self)
{
	auto cmd = command_allocate(COMMAND_PIPE, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_sort(Plan* self)
{
	auto cmd = command_allocate(COMMAND_SORT, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_union(Plan* self)
{
	auto cmd = command_allocate(COMMAND_UNION, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_union_aggs(Plan* self)
{
	auto cmd = command_allocate(COMMAND_UNION_AGGS, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_recv(Plan* self)
{
	auto cmd = command_allocate(COMMAND_RECV, sizeof(Command));
	plan_add(self, cmd);
}

static inline void
plan_add_recv_aggs(Plan* self)
{
	auto cmd = command_allocate(COMMAND_RECV_AGGS, sizeof(Command));
	plan_add(self, cmd);
}
