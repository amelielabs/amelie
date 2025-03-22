
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

static ContentType content_type[] =
{
	{ "json", 4, "application/json", 16, content_json },
	{  NULL,  0,  NULL,              0,  NULL         },
};

void
content_init(Content* self, Local* local, Buf* content)
{
	self->content      = content;
	self->content_type = NULL;
	self->local        = local;
	content_fmt_init(&self->fmt);
}

void
content_reset(Content* self)
{
	self->content_type = NULL;
	buf_reset(self->content);
}

static void
content_set_type(Content* self, Str* spec)
{
	// read format and set options
	auto fmt = &self->fmt;
	content_fmt_read(fmt, spec);

	// find and set the content type
	for (auto i = 0; content_type[i].name; i++)
	{
		auto type = &content_type[i];
		if (str_is_case(&fmt->name, type->name, type->name_size))
		{
			self->content_type = type;
			break;
		}
	}
	if (! self->content_type)
		error("unrecognized format type '%.*s'", str_size(&fmt->name),
		      str_of(&fmt->name));
}

void
content_write(Content* self, Str* spec, Columns* columns, Value* value)
{
	// set content type
	content_set_type(self, spec);

	// create content
	self->content_type->fn(self, columns, value);
}

void
content_write_json(Content* self, Str* spec, Str* name, Buf* buf)
{
	Value value;
	value_set_json_buf(&value, buf);

	// prepare columns
	Columns columns;
	columns_init(&columns);
	defer(columns_free, &columns);
	auto column = column_allocate();
	column_set_name(column, name);
	column_set_type(column, TYPE_JSON, 0);
	columns_add(&columns, column);

	// write content
	content_write(self, spec, &columns, &value);
}

void
content_write_json_error(Content* self, Error* error)
{
	// {}
	auto buf = buf_create();
	defer_buf(buf);
	encode_obj(buf);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, error->text, error->text_len);
	encode_obj_end(buf);

	uint8_t* pos = buf->start;
	json_export(self->content, NULL, &pos);

	// json
	self->content_type = &content_type[0];
}
