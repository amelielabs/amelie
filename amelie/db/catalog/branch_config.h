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

typedef struct BranchConfig BranchConfig;

struct BranchConfig
{
	Str      user;
	Str      name;
	Str      table_user;
	Str      table;
	Snapshot snapshot;
	Grants   grants;
};

static inline BranchConfig*
branch_config_allocate(void)
{
	BranchConfig* self;
	self = am_malloc(sizeof(BranchConfig));
	str_init(&self->user);
	str_init(&self->name);
	str_init(&self->table_user);
	str_init(&self->table);
	snapshot_init(&self->snapshot);
	grants_init(&self->grants);
	return self;
}

static inline void
branch_config_free(BranchConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);
	str_free(&self->table_user);
	str_free(&self->table);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
branch_config_set_user(BranchConfig* self, Str* name)
{
	str_free(&self->user);
	str_copy(&self->user, name);
}

static inline void
branch_config_set_name(BranchConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
branch_config_set_table_user(BranchConfig* self, Str* name)
{
	str_free(&self->table_user);
	str_copy(&self->table_user, name);
}

static inline void
branch_config_set_table(BranchConfig* self, Str* name)
{
	str_free(&self->table);
	str_copy(&self->table, name);
}

static inline BranchConfig*
branch_config_copy(BranchConfig* self)
{
	auto copy = branch_config_allocate();
	branch_config_set_user(copy, &self->user);
	branch_config_set_name(copy, &self->name);
	branch_config_set_table_user(copy, &self->table_user);
	branch_config_set_table(copy, &self->table);
	snapshot_copy(&copy->snapshot, &self->snapshot);
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline BranchConfig*
branch_config_read(uint8_t** pos)
{
	auto self = branch_config_allocate();
	errdefer(branch_config_free, self);
	uint8_t* pos_snapshot = NULL;
	uint8_t* pos_grants   = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "user",       &self->user       },
		{ DECODE_STRING, "name",       &self->name       },
		{ DECODE_STRING, "table_user", &self->table_user },
		{ DECODE_STRING, "table",      &self->table      },
		{ DECODE_OBJ,    "snapshot",   &pos_snapshot     },
		{ DECODE_ARRAY,  "grants",     &pos_grants       },
		{ 0,              NULL,         NULL             },
	};
	decode_obj(obj, "branch", pos);
	snapshot_read(&self->snapshot, &pos_snapshot);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
branch_config_write(BranchConfig* self, Buf* buf, int flags)
{
	// {}
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_string(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// table_user
	encode_raw(buf, "table_user", 10);
	encode_string(buf, &self->table_user);

	// table
	encode_raw(buf, "table", 5);
	encode_string(buf, &self->table);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// snapshot
	encode_raw(buf, "snapshot", 8);
	snapshot_write(&self->snapshot, buf, flags);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

	encode_obj_end(buf);
}
