#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ViewConfig ViewConfig;

struct ViewConfig
{
	Str schema;
	Str name;
	Str select;
	Def def;
};

static inline ViewConfig*
view_config_allocate(void)
{
	ViewConfig* self;
	self = mn_malloc(sizeof(ViewConfig));
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->select);
	def_init(&self->def);
	return self;
}

static inline void
view_config_free(ViewConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->select);
	def_free(&self->def);
	mn_free(self);
}

static inline void
view_config_set_schema(ViewConfig* self, Str* schema)
{
	str_copy(&self->schema, schema);
}

static inline void
view_config_set_name(ViewConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
view_config_set_select(ViewConfig* self, Str* select)
{
	str_copy(&self->select, select);
}

static inline ViewConfig*
view_config_copy(ViewConfig* self)
{
	auto copy = view_config_allocate();
	guard(copy_guard, view_config_free, copy);
	view_config_set_schema(copy, &self->schema);
	view_config_set_name(copy, &self->name);
	view_config_set_select(copy, &self->select);
	def_copy(&copy->def, &self->def);
	return unguard(&copy_guard);
}

static inline ViewConfig*
view_config_read(uint8_t** pos)
{
	auto self = view_config_allocate();
	guard(self_guard, view_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// schema
	data_skip(pos);
	data_read_string_copy(pos, &self->schema);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// select
	data_skip(pos);
	data_read_string_copy(pos, &self->select);

	// def
	data_skip(pos);
	def_read(&self->def, pos);

	return unguard(&self_guard);
}

static inline void
view_config_write(ViewConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// select
	encode_raw(buf, "select", 6);
	buf_write_str(buf, &self->select);

	// def
	encode_raw(buf, "def", 3);
	def_write(&self->def, buf);
}
