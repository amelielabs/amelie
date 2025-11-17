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
plan_add_scan_as(Plan*     self,
                 CommandId id,
                 Ast*      expr_limit,
                 Ast*      expr_offset,
                 Ast*      expr_where,
                 From*     from)
{
	auto cmd = (CommandScan*)command_allocate(id, sizeof(CommandScan));
	plan_add(self, &cmd->cmd);
	cmd->expr_limit  = expr_limit;
	cmd->expr_offset = expr_offset;
	cmd->expr_where  = expr_where;
	cmd->from        = from;
}

static inline void
plan_add_scan(Plan* self,
              Ast*  expr_limit,
              Ast*  expr_offset,
              Ast*  expr_where,
              From* from)
{
	plan_add_scan_as(self, COMMAND_SCAN, expr_limit, expr_offset,
	                 expr_where, from);
}

static inline void
plan_add_scan_ordered(Plan* self,
                      Ast*  expr_limit,
                      Ast*  expr_offset,
                      Ast*  expr_where,
                      From* from)
{
	plan_add_scan_as(self, COMMAND_SCAN_ORDERED, expr_limit, expr_offset,
	                 expr_where, from);
}

static inline void
plan_add_scan_aggs(Plan* self)
{
	auto cmd = command_allocate(COMMAND_SCAN_AGGS, sizeof(Command));
	plan_add(self, cmd);
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
plan_add_set_agg_merge(Plan* self)
{
	auto cmd = command_allocate(COMMAND_SET_AGG_MERGE, sizeof(Command));
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
