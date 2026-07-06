
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
#include <amelie_func.h>

void no_return
call_error(Call* self, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_format(buf, "{str}(", &self->function->name);
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_write(buf, ", ", 2);
		buf_format(buf, "{s}", type_of(self->argv[i].type));
	}
	buf_format(buf, ")");

	va_list args;
	char msg[256];
	va_start(args, fmt);
	formatv(msg, sizeof(msg), fmt, args);
	va_end(args);

	error("{buf} ⟵ {s}", buf, msg);
}

void no_return
call_error_noargs(Call* self, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_format(buf, "{str}()", &self->function->name);

	va_list args;
	char msg[256];
	va_start(args, fmt);
	formatv(msg, sizeof(msg), fmt, args);
	va_end(args);

	error("{buf} ⟵ {s}", buf, msg);
}

void no_return
call_error_at(Call* self, int arg, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_format(buf, "{str}(", &self->function->name);
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_write(buf, ", ", 2);
		if (i == arg) {
			buf_format(buf, "❰{s}❱", type_of(self->argv[i].type));
			break;
		}
		buf_format(buf, "{s}", type_of(self->argv[i].type));
	}

	va_list args;
	char msg[256];
	va_start(args, fmt);
	formatv(msg, sizeof(msg), fmt, args);
	va_end(args);

	error("{buf} ⟵ {s}", buf, msg);
}
