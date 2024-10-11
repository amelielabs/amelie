
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>

typedef struct
{
	uint8_t* pos;
	char*    path_pos;
	char*    path_end;
	bool     found;
	Value*   value;
	Buf*     buf;
} Update;

static inline void
update_init(Update* self, uint8_t* pos, Str* path, Buf* buf, Value* value)
{
	self->pos      = pos;
	self->path_pos = path->pos;
	self->path_end = path->end;
	self->found    = false;
	self->value    = value;
	self->buf      = buf;
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
	if (unlikely(! data_is_obj(*pos)))
		error("unset: object expected, but got %s", type_to_string(**pos));
	data_read_obj(pos);

	encode_obj(buf);
	while (! data_read_obj_end(pos))
	{
		// key
		Str at;
		data_read_string(pos, &at);

		// match key
		if (str_compare(&key, &at))
		{
			if (update_has(self))
			{
				encode_string(buf, &at);
				update_unset_next(self);
				continue;
			}
			data_skip(pos);
			continue;
		}

		// copy
		encode_string(buf, &at);
		uint8_t* start = *pos;
		data_skip(pos);
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
	if (unlikely(! data_is_obj(*pos)))
		error("set: object expected, but got %s", type_to_string(**pos));
	data_read_obj(pos);

	encode_obj(buf);
	while (! data_read_obj_end(pos))
	{
		// key 
		Str at;
		data_read_string(pos, &at);
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
			value_write(self->value, buf);
			self->found = true;
			data_skip(pos);
			continue;
		}

		// value
		uint8_t* start = *pos;
		data_skip(pos);
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
		value_write(self->value, buf);
	}
	encode_obj_end(buf);
}

hot void
update_set(Value* result, uint8_t* data, Str* path, Value* value)
{
	auto buf = buf_create();
	guard_buf(buf);
	Update self;
	update_init(&self, data, path, buf, value);
	update_set_next(&self);
	unguard();
	value_set_obj_buf(result, buf);
}

hot void
update_unset(Value* result, uint8_t* data, Str* path)
{
	auto buf = buf_create();
	guard_buf(buf);
	Update self;
	update_init(&self, data, path, buf, NULL);
	update_unset_next(&self);
	unguard();
	value_set_obj_buf(result, buf);
}

static inline void
update_set_array_next(Update* self, int idx)
{
	auto buf = self->buf;
	auto pos = &self->pos;
	if (unlikely(! data_is_array(*pos)))
		error("set: array expected, but got %s", type_to_string(**pos));
	data_read_array(pos);

	encode_array(buf);
	int at = 0;
	for (; !data_read_array_end(pos); at++)
	{
		// replace
		if (at == idx)
		{
			// [idx].path
			if (update_has(self))
			{
				update_set_next(self);
			} else
			{
				// replace value
				value_write(self->value, buf);
				assert(! self->found);
				self->found = true;
				data_skip(pos);
			}
			continue;
		}

		uint8_t* start = *pos;
		data_skip(pos);
		buf_write(buf, start, *pos - start);
	}
	if (idx >= at)
		error("set: array position is out of bounds");
	encode_array_end(buf);
}

hot void
update_set_array(Value* result, uint8_t* data, int idx, Str* path, Value* value)
{
	auto buf = buf_create();
	guard_buf(buf);
	Update self;
	update_init(&self, data, path, buf, value);
	update_set_array_next(&self, idx);
	unguard();
	value_set_array_buf(result, buf);
}
