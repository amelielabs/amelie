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

typedef struct ProcConfig ProcConfig;

struct ProcConfig
{
	Str     schema;
	Str     name;
	Str     text;
	Columns columns;
	Columns vars;
};

static inline ProcConfig*
proc_config_allocate(void)
{
	ProcConfig* self;
	self = am_malloc(sizeof(ProcConfig));
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->text);
	columns_init(&self->columns);
	columns_init(&self->vars);
	return self;
}

static inline void
proc_config_free(ProcConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->text);
	columns_free(&self->columns);
	columns_free(&self->vars);
	am_free(self);
}

static inline void
proc_config_set_schema(ProcConfig* self, Str* schema)
{
	str_free(&self->schema);
	str_copy(&self->schema, schema);
}

static inline void
proc_config_set_name(ProcConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
proc_config_set_text(ProcConfig* self, Str* text)
{
	str_copy(&self->text, text);
}

static inline ProcConfig*
proc_config_copy(ProcConfig* self)
{
	auto copy = proc_config_allocate();
	proc_config_set_schema(copy, &self->schema);
	proc_config_set_name(copy, &self->name);
	proc_config_set_text(copy, &self->text);
	columns_copy(&copy->columns, &self->columns);
	columns_copy(&copy->vars, &self->vars);
	return copy;
}

static inline ProcConfig*
proc_config_read(uint8_t** pos)
{
	auto self = proc_config_allocate();
	errdefer(proc_config_free, self);
	uint8_t* columns = NULL;
	uint8_t* vars = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "schema",  &self->schema },
		{ DECODE_STRING, "name",    &self->name   },
		{ DECODE_STRING, "text",    &self->text   },
		{ DECODE_ARRAY,  "columns", &columns      },
		{ DECODE_ARRAY,  "vars",    &vars         },
		{ 0,              NULL,      NULL         },
	};
	decode_obj(obj, "proc", pos);
	columns_read(&self->columns, &columns);
	columns_read(&self->vars, &vars);
	return self;
}

static inline void
proc_config_write(ProcConfig* self, Buf* buf)
{
	// map
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// text
	encode_raw(buf, "text", 4);
	encode_string(buf, &self->text);

	// columns
	encode_raw(buf, "columns", 7);
	columns_write(&self->columns, buf);

	// vars
	encode_raw(buf, "vars", 4);
	columns_write(&self->vars, buf);

	encode_obj_end(buf);
}
