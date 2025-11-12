
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_slt.h>

void
slt_parser_init(SltParser* self)
{
	self->data = NULL;
	self->stream_line = 0;
	str_init(&self->stream);
	slt_cmd_init(&self->cmd);
}

void
slt_parser_free(SltParser* self)
{
	if (self->data)
		buf_free(self->data);
}

void
slt_parser_open(SltParser* self, Str* path)
{
	self->data = file_import("%.*s", (int)str_size(path), str_of(path));
	buf_str(self->data, &self->stream);
}

static void
slt_parser_read(SltParser* self)
{
	// command \n
	// [line] \n | eof
	//  ...
	// [----]
	// [line] \n | eof
	//  ...
	// \n | eof
	//
	auto stream = &self->stream;
	auto cmd    = &self->cmd;
	auto body   = &cmd->body;
	str_set(body, stream->pos, 0);

	// command
	cmd->line = self->stream_line;
	if (! str_gets(stream, &cmd->cmd))
		return;
	self->stream_line++;

	// [query]
	auto query = &cmd->query;
	str_set(query, stream->pos, 0);
	for (;;)
	{
		// eof
		Str line;
		if (! str_gets(stream, &line))
			return;
		self->stream_line++;

		// \n
		if (str_empty(&line))
			return;

		// ----
		if (str_is(&line, "----", 4))
			break;

		// line (including \n)
		auto end = line.end + 1;
		query->end = end;
		body->end = end;
	}

	// [result]
	auto result = &cmd->result;
	str_set(result, stream->pos, 0);
	for (;;)
	{
		// eof
		Str line;
		if (! str_gets(stream, &line))
			return;
		self->stream_line++;

		// \n
		if (str_empty(&line))
			return;

		// line (including \n)
		auto end = line.end + 1;
		result->end = end;
		body->end = end;
	}
}

static inline void
slt_parser_match(SltParser* self)
{
	auto cmd = &self->cmd;

	// query columns sort
	if (str_is_prefix(&cmd->cmd, "query", 5))
	{
		cmd->type = SLT_QUERY;

		Str argv = cmd->cmd;
		Str columns;
		Str sort;
		str_arg(&argv, NULL);
		str_arg(&argv, &columns);
		str_arg(&argv, &sort);

		// set columns
		if (str_empty(&columns))
			slt_cmd_error_invalid(cmd);
		cmd->columns = str_size(&columns);

		// set sort
		if (str_is(&sort, "nosort", 6))
			cmd->sort = SLT_SORT_NONE;
		else
		if (str_is(&sort, "rowsort", 7))
			cmd->sort = SLT_SORT_ROW;
		else
		if (str_is(&sort, "valuesort", 9))
			cmd->sort = SLT_SORT_VALUE;
		else
			slt_cmd_error_invalid(cmd);
		return;
	}

	// statement ok | error
	if (str_is_prefix(&cmd->cmd, "statement", 9))
	{
		Str argv = cmd->cmd;
		Str arg;
		str_arg(&argv, NULL);
		str_arg(&argv, &arg);

		if (str_is(&arg, "ok", 2))
			cmd->type = SLT_STATEMENT_OK;
		else
		if (str_is(&arg, "error", 5))
			cmd->type = SLT_STATEMENT_ERROR;
		else
			slt_cmd_error_invalid(cmd);
		return;
	}

	// hash-threshold value
	if (str_is_prefix(&cmd->cmd, "hash-threshold", 14))
	{
		cmd->type = SLT_HASH_THRESHOLD;

		Str argv = cmd->cmd;
		Str arg;
		str_arg(&argv, NULL);
		str_arg(&argv, &arg);

		// set value
		int64_t value = 0;
		if (str_empty(&arg) || str_toint(&arg, &value) == -1)
			slt_cmd_error_invalid(cmd);
		cmd->value = value;
		return;
	}

	// halt
	if (str_is_prefix(&cmd->cmd, "halt", 4))
	{
		cmd->type = SLT_HALT;
		return;
	}

	// undef
	cmd->type = SLT_UNDEF;
}

SltCmd*
slt_parser_next(SltParser* self)
{
	auto cmd = &self->cmd;
	slt_cmd_reset(cmd);

	// todo: [skipif | onlyif \n]
	slt_parser_read(self);

	// eof
	if (str_empty(&cmd->cmd))
		return NULL;

	// find command
	slt_parser_match(self);
	return cmd;
}
