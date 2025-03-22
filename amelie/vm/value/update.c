
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

typedef struct
{
	uint8_t*  pos;
	char*     path_pos;
	char*     path_end;
	bool      found;
	Value*    value;
	Buf*      buf;
	Timezone* tz;
} Update;

static inline void
update_init(Update*  self, Timezone* tz,
            uint8_t* pos,
            Str*     path,
            Buf*     buf,
            Value*   value)
{
	self->pos      = pos;
	self->path_pos = path->pos;
	self->path_end = path->end;
	self->found    = false;
	self->value    = value;
	self->buf      = buf;
	self->tz       = tz;
}

static inline bool
update_has(Update* self)
{
	return self->path_pos < self->path_end;
}

static inline void
update_read(Update* self, Str* name)
{
	if (unlikely(! update_has(self)))
		error("set: incorrect path");
	auto start = self->path_pos;
	while (self->path_pos < self->path_end)
	{
		if (*self->path_pos == '.')
			break;
		if (unlikely(isspace(*self->path_pos)))
			error("set: incorrect path");
		self->path_pos++;
	}
	str_set(name, start, self->path_pos - start);
	if (unlikely(! str_size(name)))
		error("set: incorrect path");
	if (self->path_pos != self->path_end)
		self->path_pos++;
}

static inline void
update_unset_next(Update* self)
{
	// read next key in the update path
	Str key;
	update_read(self, &key);

	auto buf = self->buf;
	auto pos = &self->pos;
	if (unlikely(! json_is_obj(*pos)))
		error("unset: object expected, but got %s", json_typeof(**pos));
	json_read_obj(pos);

	encode_obj(buf);
	while (! json_read_obj_end(pos))
	{
		// key
		Str at;
		json_read_string(pos, &at);

		// match key
		if (str_compare(&key, &at))
		{
			if (update_has(self))
			{
				encode_string(buf, &at);
				update_unset_next(self);
				continue;
			}
			json_skip(pos);
			continue;
		}

		// copy
		encode_string(buf, &at);
		uint8_t* start = *pos;
		json_skip(pos);
		buf_write(buf, start, *pos - start);
	}
	encode_obj_end(buf);
}

static inline void
update_set_next(Update* self)
{
	// read next key in the update path
	Str key;
	update_read(self, &key);

	auto buf = self->buf;
	auto pos = &self->pos;
	if (unlikely(! json_is_obj(*pos)))
		error("set: object expected, but got %s", json_typeof(**pos));
	json_read_obj(pos);

	encode_obj(buf);
	while (! json_read_obj_end(pos))
	{
		// key 
		Str at;
		json_read_string(pos, &at);
		encode_string(buf, &at);

		// match key
		if (!self->found && str_compare(&key, &at))
		{
			// path.path
			if (update_has(self))
			{
				update_set_next(self);
				continue;
			}

			// replace value
			assert(! self->found);
			value_encode(self->value, self->tz, buf);
			self->found = true;
			json_skip(pos);
			continue;
		}

		// value
		uint8_t* start = *pos;
		json_skip(pos);
		buf_write(buf, start, *pos - start);
	}

	// append to set
	if (! self->found)
	{
		if (update_has(self))
			error("set: incorrect path");

		// key
		encode_string(buf, &key);

		// value
		value_encode(self->value, self->tz, buf);
	}
	encode_obj_end(buf);
}

hot void
update_set(Value* result, Timezone* tz, uint8_t* json, Str* path, Value* value)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	Update self;
	update_init(&self, tz, json, path, buf, value);
	update_set_next(&self);
	value_set_json_buf(result, buf);
}

hot void
update_unset(Value* result, uint8_t* json, Str* path)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	Update self;
	update_init(&self, NULL, json, path, buf, NULL);
	update_unset_next(&self);
	value_set_json_buf(result, buf);
}
