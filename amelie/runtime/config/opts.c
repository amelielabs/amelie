
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

void
opts_init(Opts* self)
{
	self->list_count++;
	list_init(&self->list);
}

void
opts_free(Opts* self)
{
	list_foreach_safe(&self->list)
	{
		auto opt = list_at(Opt, link);
		opt_free(opt);
	}
}

static inline void
opts_add(Opts* self, Opt* opt)
{
	list_append(&self->list, &opt->link);
	self->list_count++;
}

void
opts_define(Opts* self, OptsDef* defs)
{
	for (int i = 0; defs[i].name != NULL; i++)
	{
		auto def = &defs[i];
		auto opt = def->opt;
		opt_init(opt, def->name, def->type, def->flags);
		opts_add(self, opt);
		switch (def->type) {
		case OPT_BOOL:
		case OPT_INT:
			opt_int_set(opt, def->default_int);
			break;
		case OPT_STRING:
			if (def->default_string)
				opt_string_set_raw(opt, def->default_string, strlen(def->default_string));
			break;
		case OPT_JSON:
			break;
		}
	}
}

Opt*
opts_find(Opts* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto opt = list_at(Opt, link);
		if (str_compare(&opt->name, name))
			return opt;
	}
	return NULL;
}

bool
opts_set_json(Opts* self, uint8_t** pos)
{
	bool update = false;
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		// key
		Str name;
		json_read_string(pos, &name);

		// find optiable and set value
		auto opt = opts_find(self, &name);
		if (unlikely(opt == NULL))
			error("option '%.*s': not found", str_size(&name), str_of(&name));

		// ensure optiable can be changed
		if (unlikely(! opt_is(opt, OPT_C)))
			error("option '%.*s': cannot be changed", str_size(&name),
			      str_of(&name));

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
			error("option '%s': failed to parse", argv[i]);

		// --json={}
		if (str_is_cstr(&name, "json"))
		{
			if (str_empty(&value))
				error("option 'json': value is not defined");
			if (opts_set(self, &value))
				update = true;
			continue;
		}

		// --daemon (handled by runner)
		if (str_is_cstr(&name, "daemon") ||
		    str_is_cstr(&name, "daemon=true") ||
		    str_is_cstr(&name, "daemon=false"))
			continue;

		auto opt = opts_find(self, &name);
		if (unlikely(opt == NULL))
			error("option '%.*s': not found", str_size(&name), str_of(&name));

		// ensure optiable can be changed
		if (unlikely(! opt_is(opt, OPT_C)))
			error("option '%.*s': cannot be changed", str_size(&name),
			      str_of(&name));

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
	list_foreach(&self->list)
	{
		auto opt = list_at(Opt, link);
		if (opt_is(opt, OPT_E))
			continue;
		encode_string(buf, &opt->name);
		opt_encode(opt, buf);
	}
	encode_obj_end(buf);
	return buf;
}

Buf*
opts_list(Opts* self)
{
	auto buf = buf_create();
	encode_obj(buf);
	list_foreach(&self->list)
	{
		auto opt = list_at(Opt, link);
		if (opt_is(opt, OPT_H) || opt_is(opt, OPT_S))
			continue;
		encode_string(buf, &opt->name);
		opt_encode(opt, buf);
	}
	encode_obj_end(buf);
	return buf;
}

void
opts_print(Opts* self)
{
	list_foreach(&self->list)
	{
		auto opt = list_at(Opt, link);
		if (opt_is(opt, OPT_H) || opt_is(opt, OPT_S))
			continue;
		opt_print(opt);
	}
}
