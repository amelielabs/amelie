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

typedef struct LayerConfig LayerConfig;

struct LayerConfig
{
	Str      user;
	Str      name;
	Str      description;
	Str      table_user;
	Str      table;
	Timeline timeline;
	Grants   grants;
};

static inline LayerConfig*
layer_config_allocate(void)
{
	LayerConfig* self;
	self = am_malloc(sizeof(LayerConfig));
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->description);
	str_init(&self->table_user);
	str_init(&self->table);
	timeline_init(&self->timeline);
	grants_init(&self->grants);
	return self;
}

static inline void
layer_config_free(LayerConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	str_free(&self->description);
	str_free(&self->table_user);
	str_free(&self->table);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
layer_config_set_user(LayerConfig* self, Str* name)
{
	str_free(&self->user);
	str_copy(&self->user, name);
}

static inline void
layer_config_set_name(LayerConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
layer_config_set_description(LayerConfig* self, Str* value)
{
	str_free(&self->description);
	str_copy(&self->description, value);
}

static inline void
layer_config_set_table_user(LayerConfig* self, Str* name)
{
	str_free(&self->table_user);
	str_copy(&self->table_user, name);
}

static inline void
layer_config_set_table(LayerConfig* self, Str* name)
{
	str_free(&self->table);
	str_copy(&self->table, name);
}

static inline LayerConfig*
layer_config_copy(LayerConfig* self)
{
	auto copy = layer_config_allocate();
	layer_config_set_user(copy, &self->user);
	layer_config_set_name(copy, &self->name);
	layer_config_set_description(copy, &self->description);
	layer_config_set_table_user(copy, &self->table_user);
	layer_config_set_table(copy, &self->table);
	timeline_copy(&copy->timeline, &self->timeline);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline LayerConfig*
layer_config_read(uint8_t** pos)
{
	auto self = layer_config_allocate();
	errdefer(layer_config_free, self);
	uint8_t* pos_grants   = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "user",        &self->user              },
		{ DECODE_STR,   "name",        &self->name              },
		{ DECODE_STR,   "description", &self->description       },
		{ DECODE_STR,   "table_user",  &self->table_user        },
		{ DECODE_STR,   "table",       &self->table             },
		{ DECODE_INT,   "timeline",    &self->timeline.timeline },
		{ DECODE_ARRAY, "grants",      &pos_grants              },
		{ 0,             NULL,          NULL                    },
	};
	decode_obj(obj, "layer", pos);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
layer_config_write(LayerConfig* self, Buf* buf, int flags)
{
	// {}
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_str(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	// description
	encode_raw(buf, "description", 11);
	encode_str(buf, &self->description);

	// table_user
	encode_raw(buf, "table_user", 10);
	encode_str(buf, &self->table_user);

	// table
	encode_raw(buf, "table", 5);
	encode_str(buf, &self->table);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// timeline
	encode_raw(buf, "timeline", 8);
	encode_int(buf, self->timeline.timeline);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
