
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
vars_init(Vars* self)
{
	self->list_count++;
	list_init(&self->list);
}

void
vars_free(Vars* self)
{
	list_foreach_safe(&self->list)
	{
		auto var = list_at(Var, link);
		var_free(var);
	}
}

static inline void
vars_add(Vars* self, Var* var)
{
	list_append(&self->list, &var->link);
	self->list_count++;
}

void
vars_define(Vars* self, VarDef* defs)
{
	for (int i = 0; defs[i].name != NULL; i++)
	{
		auto def = &defs[i];
		auto var = def->var;
		var_init(var, def->name, def->type, def->flags);
		vars_add(self, var);
		switch (def->type) {
		case VAR_BOOL:
		case VAR_INT:
			var_int_set(var, def->default_int);
			break;
		case VAR_STRING:
			if (def->default_string)
				var_string_set_raw(var, def->default_string, strlen(def->default_string));
			break;
		case VAR_JSON:
			break;
		}
	}
}

Var*
vars_find(Vars* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (str_compare(&var->name, name))
			return var;
	}
	return NULL;
}

bool
vars_set_json(Vars* self, uint8_t** pos, bool system)
{
	bool update = false;
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		// key
		Str name;
		json_read_string(pos, &name);

		// find variable and set value
		auto var = vars_find(self, &name);
		if (unlikely(var == NULL))
			error("option '%.*s': not found", str_size(&name), str_of(&name));

		// ensure variable can be changed
		if (unlikely(! var_is(var, VAR_C)))
			error("option '%.*s': cannot be changed",
			      str_size(&name), str_of(&name));

		// system state var (can be changed only during config read)
		if (unlikely(var_is(var, VAR_Y) && !system))
			error("option '%.*s': cannot be changed",
			      str_size(&name), str_of(&name));

		// if config update will be required
		if (! var_is(var, VAR_E))
			update = true;

		// set data value based on type
		var_set_json(var, pos);
	}
	return update;
}

bool
vars_set(Vars* self, Str* options, bool system)
{
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, options, NULL);
	uint8_t* pos = json.buf->start;
	return vars_set_json(self, &pos, system);
}

bool
vars_set_argv(Vars* self, int argc, char** argv)
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
			if (vars_set(self, &value, false))
				update = true;
			continue;
		}

		// --daemon (handled by runner)
		if (str_is_cstr(&name, "daemon") ||
		    str_is_cstr(&name, "daemon=true") ||
		    str_is_cstr(&name, "daemon=false"))
			continue;

		auto var = vars_find(self, &name);
		if (unlikely(var == NULL))
			error("option '%.*s': not found", str_size(&name), str_of(&name));

		// ensure variable can be changed
		if (unlikely(!var_is(var, VAR_C) || var_is(var, VAR_Y)))
			error("option '%.*s': cannot be changed",
			      str_size(&name), str_of(&name));

		// if config update will be required
		if (! var_is(var, VAR_E))
			update = true;

		// set value based on type
		var_set(var, &value);
	}
	return update;
}

Buf*
vars_list_persistent(Vars* self)
{
	auto buf = buf_create();
	encode_obj(buf);
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (var_is(var, VAR_E))
			continue;
		encode_string(buf, &var->name);
		var_encode(var, buf);
	}
	encode_obj_end(buf);
	return buf;
}

Buf*
vars_list(Vars* self, Vars* local)
{
	auto buf = buf_create();
	encode_obj(buf);
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (var_is(var, VAR_H) || var_is(var, VAR_S))
			continue;
		encode_string(buf, &var->name);
		if (var_is(var, VAR_L))
			var = vars_find(local, &var->name);
		var_encode(var, buf);
	}
	encode_obj_end(buf);
	return buf;
}

void
vars_print(Vars* self)
{
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (var_is(var, VAR_H) || var_is(var, VAR_S))
			continue;
		var_print(var);
	}
}
