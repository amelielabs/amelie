
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

#include <amelie_base.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_lock.h>
#include <amelie_runtime.h>

void
opts_init(Opts* self)
{
	self->count = 0;
	self->opts  = NULL;
}

void
opts_free(Opts* self)
{
	opts_reset(self);
	if (self->opts)
	{
		am_free(self->opts);
		self->count = 0;
		self->opts  = NULL;
	}
}

void
opts_reset(Opts* self)
{
	for (auto i = 0; i < self->count; i++)
		opt_free(&self->opts[i]);
}

void
opts_define(Opts* self, OptsDef* defs, int count)
{
	assert(! self->opts);

	self->count = count;
	self->opts  = am_malloc(sizeof(Opt) * count);
	for (auto i = 0; i < count; i++)
	{
		auto def = &defs[i];
		auto opt = &self->opts[i];
		opt_init(opt, def->name, def->type, def->flags, def->value);
		switch (def->type) {
		case OPT_BOOL:
			*opt->boolean = def->default_int;
			break;
		case OPT_INT:
			*opt->integer = def->default_int;
			break;
		case OPT_STRING:
			str_init(opt->string);
			if (def->default_string)
				str_set_copy_cstr(opt->string, def->default_string);
			break;
		case OPT_JSON:
			str_init(opt->string);
			break;
		case OPT_UUID:
			uuid_init(opt->uuid);
			break;
		}
	}
}

Opt*
opts_find(Opts* self, Str* name)
{
	for (auto i = 0; i < self->count; i++)
		if (str_compare(&self->opts[i].name, name))
			return &self->opts[i];
	return NULL;
}

bool
opts_set_json(Opts* self, uint8_t** pos)
{
	bool update = false;
	unpack_obj(pos);
	while (! unpack_obj_end(pos))
	{
		// key
		Str name;
		unpack_str(pos, &name);

		// find options and set value
		auto opt = opts_find(self, &name);
		if (unlikely(opt == NULL))
			error("option '{str}': not found", &name);

		// ensure options can be changed
		if (unlikely(! opt_is(opt, OPT_C)))
			error("option '{str}': cannot be changed", &name);

		// if config update will be required
		if (! opt_is(opt, OPT_E))
			update = true;

		// set data value based on type
		opt_set_json(opt, pos);
	}
	return update;
}

bool
opts_set(Opts* self, Str* options)
{
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, options, NULL);
	uint8_t* pos = json.buf->start;
	return opts_set_json(self, &pos);
}

bool
opts_set_argv(Opts* self, int argc, char** argv)
{
	bool update = false;
	for (int i = 0; i < argc; i++)
	{
		Str name;
		Str value;
		if (arg_parse(argv[i], &name, &value) == -1)
			error("option '{s}': failed to parse", argv[i]);

		// --json={}
		if (str_is_cstr(&name, "json"))
		{
			if (str_empty(&value))
				error("option 'json': value is not defined");
			if (opts_set(self, &value))
				update = true;
			continue;
		}

		auto opt = opts_find(self, &name);
		if (unlikely(opt == NULL))
			error("option '{str}': not found", &name);

		// ensure options can be changed
		if (unlikely(! opt_is(opt, OPT_C)))
			error("option '{str}': cannot be changed", &name);

		// if config update will be required
		if (! opt_is(opt, OPT_E))
			update = true;

		// set value based on type
		opt_set(opt, &value);
	}
	return update;
}

Buf*
opts_list_persistent(Opts* self)
{
	auto buf = buf_create();
	encode_obj(buf);
	for (auto i = 0; i < self->count; i++)
	{
		auto pos = &self->opts[i];
		if (opt_is(pos, OPT_E))
			continue;
		encode_str(buf, &pos->name);
		opt_encode(pos, buf);
	}
	encode_obj_end(buf);
	return buf;
}

Buf*
opts_list(Opts* self)
{
	auto buf = buf_create();
	encode_obj(buf);
	for (auto i = 0; i < self->count; i++)
	{
		auto pos = &self->opts[i];
		if (opt_is(pos, OPT_H) || opt_is(pos, OPT_S))
			continue;
		encode_str(buf, &pos->name);
		opt_encode(pos, buf);
	}
	encode_obj_end(buf);
	return buf;
}

void
opts_print(Opts* self)
{
	for (auto i = 0; i < self->count; i++)
	{
		auto pos = &self->opts[i];
		if (opt_is(pos, OPT_H) || opt_is(pos, OPT_S))
			continue;
		opt_print(pos);
	}
}
