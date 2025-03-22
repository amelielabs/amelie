
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

void no_return
call_error(Call* self, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_printf(buf, "%.*s(", str_size(&self->function->name),
	           str_of(&self->function->name));
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_printf(buf, ", ");
		buf_printf(buf, "%s", type_of(self->argv[i].type));
	}
	buf_printf(buf, ")");

	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);

	error("%.*s ⟵ %s", buf_size(buf), buf->start, msg);
}

void no_return
call_error_arg(Call* self, int arg, char* fmt, ...)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	buf_printf(buf, "%.*s(", str_size(&self->function->name),
	           str_of(&self->function->name));
	for (auto i = 0; i < self->argc; i++)
	{
		if (i > 0)
			buf_printf(buf, ", ");
		if (i == arg) {
			buf_printf(buf, "❰%s❱", type_of(self->argv[i].type));
			break;
		}
		buf_printf(buf, "%s", type_of(self->argv[i].type));
	}

	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);

	error("%.*s ⟵ %s", buf_size(buf), buf->start, msg);
}
