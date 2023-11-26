#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct PartConfig PartConfig;

struct PartConfig
{
	Uuid    id;
	Uuid    id_table;
	Uuid    id_shard;
	int64_t range_start;
	int64_t range_end;
};

static inline PartConfig*
part_config_allocate(void)
{
	PartConfig* self;
	self = mn_malloc(sizeof(PartConfig));
	self->range_start = 0;
	self->range_end   = 0;
	uuid_init(&self->id);
	uuid_init(&self->id_table);
	uuid_init(&self->id_shard);
	return self;
}

static inline void
part_config_free(PartConfig* self)
{
	mn_free(self);
}

static inline void
part_config_set_id(PartConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
part_config_set_id_table(PartConfig* self, Uuid* id)
{
	self->id_table = *id;
}

static inline void
part_config_set_id_shard(PartConfig* self, Uuid* id)
{
	self->id_shard = *id;
}

static inline void
part_config_set_range(PartConfig* self, int start, int end)
{
	self->range_start = start;
	self->range_end   = end;
}

static inline PartConfig*
part_config_copy(PartConfig* self)
{
	auto copy = part_config_allocate();
	guard(copy_guard, part_config_free, copy);
	part_config_set_id(copy, &self->id);
	part_config_set_id_table(copy, &self->id_table);
	part_config_set_id_shard(copy, &self->id_shard);
	part_config_set_range(copy, self->range_start, self->range_end);
	return unguard(&copy_guard);
}

static inline PartConfig*
part_config_read(uint8_t** pos)
{
	auto self = part_config_allocate();
	guard(self_guard, part_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// id
	data_skip(pos);
	Str id;
	data_read_string(pos, &id);
	uuid_from_string(&self->id, &id);

	// id_table
	data_skip(pos);
	data_read_string(pos, &id);
	uuid_from_string(&self->id_table, &id);

	// id_shard
	data_skip(pos);
	data_read_string(pos, &id);
	uuid_from_string(&self->id_shard, &id);

	// range_start
	data_skip(pos);
	data_read_integer(pos, &self->range_start);

	// range_end
	data_skip(pos);
	data_read_integer(pos, &self->range_end);
	return unguard(&self_guard);
}

static inline void
part_config_write(PartConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 5);

	// id
	encode_raw(buf, "id", 2);
	char uuid[UUID_SZ];
	uuid_to_string(&self->id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// id_parent
	encode_raw(buf, "id_table", 8);
	uuid_to_string(&self->id_table, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// id_shard
	encode_raw(buf, "id_shard", 8);
	uuid_to_string(&self->id_shard, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// range_start
	encode_raw(buf, "range_start", 11);
	encode_integer(buf, self->range_start);

	// range_end
	encode_raw(buf, "range_end", 9);
	encode_integer(buf, self->range_end);
}
