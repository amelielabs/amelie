
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
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>

static inline bool
engine_create_directory(Engine* self)
{
	// <base>/<id>
	char uuid[UUID_SZ];
	uuid_to_string(self->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s", config_directory(), uuid);
	if (fs_exists("%s", path))
		return true;

	log("engine: new directory '%s'", path);
	fs_mkdir(0755, "%s", path);
	return false;
}

void
engine_recover(Engine* self)
{
	// create directory if not exists
	engine_create_directory(self);

	// todo: read directory

	// create first partition, if necessary
	if (list_empty(&self->list))
	{
		uint64_t id = config_psn_next();
		auto part = part_allocate(self->schema, self->id, id, id, 0, ROW_PARTITION_MAX);
		list_append(&self->list, &part->link);
		self->list_count++;
	} else
	{
		// todo
	}

	// prepare partition map
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_map_set(&self->map, part);
	}
}
