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

typedef struct CloneConfig CloneConfig;

struct CloneConfig
{
	Str      user;
	Str      name;
	Str      description;
	Uuid     id;
	Str      table_user;
	Str      table;
	Snapshot snapshot;
	Grants   grants;
};

static inline CloneConfig*
clone_config_allocate(void)
{
	CloneConfig* self;
	self = am_malloc(sizeof(CloneConfig));
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->description);
	str_init(&self->table_user);
	str_init(&self->table);
	uuid_init(&self->id);
	snapshot_init(&self->snapshot);
	grants_init(&self->grants);
	return self;
}

static inline void
clone_config_free(CloneConfig* self)
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
clone_config_set_user(CloneConfig* self, Str* name)
{
	str_free(&self->user);
	str_copy(&self->user, name);
}

static inline void
clone_config_set_name(CloneConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
clone_config_set_description(CloneConfig* self, Str* value)
{
	str_free(&self->description);
	str_copy(&self->description, value);
}

static inline void
clone_config_set_id(CloneConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
clone_config_set_table_user(CloneConfig* self, Str* name)
{
	str_free(&self->table_user);
	str_copy(&self->table_user, name);
}

static inline void
clone_config_set_table(CloneConfig* self, Str* name)
{
	str_free(&self->table);
	str_copy(&self->table, name);
}

static inline CloneConfig*
clone_config_copy(CloneConfig* self)
{
	auto copy = clone_config_allocate();
	clone_config_set_user(copy, &self->user);
	clone_config_set_name(copy, &self->name);
	clone_config_set_description(copy, &self->description);
	clone_config_set_id(copy, &self->id);
	clone_config_set_table_user(copy, &self->table_user);
	clone_config_set_table(copy, &self->table);
	snapshot_copy(&copy->snapshot, &self->snapshot);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline CloneConfig*
clone_config_read(uint8_t** pos)
{
	auto self = clone_config_allocate();
	errdefer(clone_config_free, self);
	uint8_t* pos_snapshot = NULL;
	uint8_t* pos_grants   = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "user",        &self->user        },
		{ DECODE_STR,   "name",        &self->name        },
		{ DECODE_STR,   "description", &self->description },
		{ DECODE_UUID,  "id",          &self->id          },
		{ DECODE_STR,   "table_user",  &self->table_user  },
		{ DECODE_STR,   "table",       &self->table       },
		{ DECODE_OBJ,   "snapshot",    &pos_snapshot      },
		{ DECODE_ARRAY, "grants",      &pos_grants        },
		{ 0,             NULL,          NULL              },
	};
	decode_obj(obj, "clone", pos);
	snapshot_read(&self->snapshot, &pos_snapshot);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
clone_config_write(CloneConfig* self, Buf* buf, int flags)
{
	// {}
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_str(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	// descriptioon
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

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// snapshot
	encode_raw(buf, "snapshot", 8);
	snapshot_write(&self->snapshot, buf, flags);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
