
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>

typedef struct
{
	char     *name;
	VarType   type;
	int       flags;
	Var      *var;
	char     *default_string;
	uint64_t  default_int;
} ConfigDef;

static inline void
config_add(Config* self, ConfigDef* def)
{
	auto var = def->var;
	var_init(var, def->name, def->type, def->flags);
	list_append(&self->list, &var->link);
	self->count++;

	if (! (var_is(var, VAR_H) || var_is(var, VAR_H)))
		self->count_visible++;
	if (var_is(var, VAR_C))
		self->count_config++;
	if (var_is(var, VAR_P))
	{
		list_append(&self->list_persistent, &var->link_persistent);
		self->count_persistent++;
	}

	switch (def->type) {
	case VAR_BOOL:
	case VAR_INT:
		var_int_set(var, def->default_int);
		break;
	case VAR_STRING:
		if (def->default_string)
			var_string_set_raw(var, def->default_string, strlen(def->default_string));
		break;
	case VAR_DATA:
		break;
	}
}

void
config_prepare(Config* self)
{
	ConfigDef defaults[] =
	{
		{ "version",                 VAR_STRING, 0,                    &self->version,                 "0.0",       0                },
		{ "uuid",                    VAR_STRING, VAR_C|VAR_P,          &self->uuid,                    NULL,        0                },
		{ "directory",               VAR_STRING, 0,                    &self->directory,               NULL,        0                },
		{ "backup",                  VAR_STRING, VAR_C,                &self->backup,                  NULL,        0                },
		{ "log_enable",              VAR_BOOL,   VAR_C,                &self->log_enable,              NULL,        true             },
		{ "log_to_file",             VAR_BOOL,   VAR_C,                &self->log_to_file,             NULL,        true             },
		{ "log_to_stdout",           VAR_BOOL,   VAR_C,                &self->log_to_stdout,           NULL,        false            },
		{ "log_connections",         VAR_BOOL,   VAR_C|VAR_R,          &self->log_connections,         NULL,        true             },
		{ "log_query",               VAR_BOOL,   VAR_C|VAR_R,          &self->log_query,               NULL,        false            },
		{ "tls_cert",                VAR_STRING, VAR_C,                &self->tls_cert,                NULL,        0                },
		{ "tls_key",                 VAR_STRING, VAR_C,                &self->tls_key,                 NULL,        0                },
		{ "tls_ca",                  VAR_STRING, VAR_C,                &self->tls_ca,                  NULL,        0                },
		{ "listen",                  VAR_DATA,   VAR_C,                &self->listen,                  NULL,        0                },
		{ "engine_workers",          VAR_INT,    VAR_C|VAR_R,          &self->engine_workers,          NULL,        1                },
		{ "engine_partition",        VAR_INT,    VAR_C,                &self->engine_partition,        NULL,        1061008          },
		{ "engine_split",            VAR_INT,    VAR_C,                &self->engine_split,            NULL,        50               },
		{ "wal_rotate_wm",           VAR_INT,    VAR_C|VAR_R,          &self->wal_rotate_wm,           NULL,        10485760         },
		{ "wal_sync_on_rotate",      VAR_BOOL,   VAR_C,                &self->wal_sync_on_rotate,      NULL,        false            },
		{ "wal_sync_on_write",       VAR_BOOL,   VAR_C,                &self->wal_sync_on_write,       NULL,        false            },
		{ "repl_enable",             VAR_BOOL,   VAR_P,                &self->repl_enable,             0,           false            },
		{ "repl_reconnect_ms",       VAR_INT,    VAR_C,                &self->repl_reconnect_ms,       NULL,        3000             },
		{ "repl_primary",            VAR_STRING, VAR_P,                &self->repl_primary,            NULL,        0                },
		{ "repl_role",               VAR_STRING, VAR_P,                &self->repl_role,               "primary",   0                },
		{ "lsn",                     VAR_INT,    0,                    &self->lsn,                     NULL,        0                },
		{ "psn",                     VAR_INT,    0,                    &self->psn,                     NULL,        0                },
		{ "read_only",               VAR_BOOL,   0,                    &self->read_only,               NULL,        false            },
		{ "users",                   VAR_DATA,   VAR_H|VAR_P,          &self->users,                   NULL,        0                },
		{ "nodes",                   VAR_DATA,   VAR_H|VAR_P,          &self->nodes,                   NULL,        0                },
		{ "catalog",                 VAR_DATA,   VAR_H|VAR_P,          &self->catalog,                 NULL,        0                },
		{ "catalog_snapshot",        VAR_INT,    VAR_H|VAR_P,          &self->catalog_snapshot,        NULL,        0                },
		// testing
		{ "test_bool",               VAR_BOOL,   VAR_H|VAR_R,          &self->test_bool,               NULL,        false            },
		{ "test_int",                VAR_INT,    VAR_H|VAR_R,          &self->test_int,                NULL,        0                },
		{ "test_string",             VAR_STRING, VAR_H|VAR_R,          &self->test_string,             NULL,        0                },
		{ "test_data",               VAR_DATA,   VAR_H|VAR_R,          &self->test_data,               NULL,        0                },
		{ "error_engine_merger_1",   VAR_BOOL,   VAR_H|VAR_R,          &self->error_engine_merger_1,   NULL,        false            },
		{ "error_engine_merger_2",   VAR_BOOL,   VAR_H|VAR_R,          &self->error_engine_merger_2,   NULL,        false            },
		{ "error_engine_merger_3",   VAR_BOOL,   VAR_H|VAR_R,          &self->error_engine_merger_3,   NULL,        false            },
		{ "error_engine_merger_4",   VAR_BOOL,   VAR_H|VAR_R,          &self->error_engine_merger_4,   NULL,        false            },
		{  NULL,                     0,             0,                 NULL,                           NULL,        0                },
	};
	for (int i = 0; defaults[i].name != NULL; i++)
		config_add(self, &defaults[i]);
}

void
config_init(Config* self)
{
	memset(self, 0, sizeof(*self));
	list_init(&self->list);
	list_init(&self->list_persistent);
}

void
config_free(Config* self)
{
	list_foreach_safe(&self->list) {
		auto var = list_at(Var, link);
		var_free(var);
	}
}

void
config_set_data(Config* self, bool system, uint8_t** pos)
{
	int count;
	data_read_map(pos, &count);

	for (int i = 0; i < count; i++)
	{
		// key
		Str name;
		data_read_string(pos, &name);

		// find variable and set value
		auto var = config_find(self, &name);
		if (unlikely(var == NULL))
			error("config: unknown option '%.*s'",
			      str_size(&name), str_of(&name));

		if (unlikely(!var_is(var, VAR_C) && !system))
			error("config: option '%.*s' is read-only",
			      str_size(&name), str_of(&name));

		switch (var->type) {
		case VAR_BOOL:
		{
			if (unlikely(! data_is_bool(*pos)))
				error("config: bool expected for option '%.*s'",
				      str_size(&name), str_of(&name));
			bool value;
			data_read_bool(pos, &value);
			var_int_set(var, value);
			break;
		}
		case VAR_INT:
		{
			if (unlikely(! data_is_integer(*pos)))
				error("config: integer expected for option '%.*s'",
				      str_size(&name), str_of(&name));
			int64_t value;
			data_read_integer(pos, &value);
			var_int_set(var, value);
			break;
		}
		case VAR_STRING:
		{
			if (unlikely(! data_is_string(*pos)))
				error("config: string expected for option '%.*s'",
				      str_size(&name), str_of(&name));
			Str value;
			data_read_string(pos, &value);
			var_string_set(var, &value);
			break;
		}
		case VAR_DATA:
		{
			auto start = *pos;
			data_skip(pos);
			var_data_set(var, start, *pos - start);
			break;
		}
		default:
			error("config: bad option '%.*s' value",
			      str_size(&name), str_of(&name));
		}
	}
}

void
config_set(Config* self, bool system, Str* options)
{
	Json json;
	json_init(&json);
	guard(json_guard, json_free, &json);
	json_parse(&json, options);
	uint8_t* pos = json.buf->start;
	config_set_data(self, system, &pos);
}

void
config_copy(Config* self, Config* src)
{
	auto j = src->list.next;
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		auto var_src = container_of(j, Var, link);
		assert(var_src);
		switch (var->type) {
		case VAR_BOOL:
		case VAR_INT:
			var_int_set(var, var_src->integer);
			break;
		case VAR_STRING:
			var_string_set(var, &var_src->string);
			break;
		case VAR_DATA:
			var_data_set_str(var, &var_src->string);
			break;
		}
		j = j->next;
	}
}

static Buf*
config_create_default(Config* self)
{
	auto list = config_list_default(self);
	auto text = buf_create(0);
	uint8_t* pos = msg_of(list)->data;
	json_to_string_pretty(text, 0, &pos);
	buf_free(list);
	return text;
}

void
config_open(Config* self, const char* path)
{
	auto buf = file_import("%s", path);
	Str options;
	str_init(&options);
	buf_str(buf, &options);
	config_set(self, true, &options);
	buf_free(buf);
}

void
config_create(Config* self, const char* path)
{
	auto text = config_create_default(self);
	File file;
	file_init(&file);
	guard(file_guard, file_close, &file);
	file_create(&file, path);
	file_write_buf(&file, text);
}

void
config_print(Config* self)
{
	list_foreach(&self->list) {
		auto var = list_at(Var, link);
		if (var_is(var, VAR_H) || var_is(var, VAR_S))
			continue;
		var_print(var);
	}
}

Buf*
config_list(Config* self)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_map(buf, self->count_visible);
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (var_is(var, VAR_H) || var_is(var, VAR_S))
			continue;
		encode_string(buf, &var->name);
		var_msg_write(var, buf);
	}
	msg_end(buf);
	return buf;
}

Buf*
config_list_default(Config* self)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_map(buf, self->count_config);
	list_foreach(&self->list)
	{
		auto var = list_at(Var, link);
		if (! var_is(var, VAR_C))
			continue;
		encode_string(buf, &var->name);
		var_msg_write(var, buf);
	}
	msg_end(buf);
	return buf;
}

Buf*
config_list_persistent(Config* self)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_map(buf, self->count_persistent);
	list_foreach(&self->list_persistent)
	{
		auto var = list_at(Var, link_persistent);
		encode_string(buf, &var->name);
		var_msg_write(var, buf);
	}
	msg_end(buf);
	return buf;
}

Var*
config_find(Config* self, Str* name)
{
	list_foreach(&self->list) {
		auto var = list_at(Var, link);
		if (str_compare(&var->name, name))
			return var;
	}
	return NULL;
}
