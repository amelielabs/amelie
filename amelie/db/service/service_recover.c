
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

static void
service_recover_gc(uint8_t* pos)
{
	// remove all existing files from the list
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		Str path_relative;
		decode_basepath(&pos, &path_relative);

		// <base>/storage/<volume_id>/id
		char path[PATH_MAX];
		sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
		     str_size(&path_relative),
		     str_of(&path_relative));

		if (fs_exists("%s", path))
			fs_unlink("%s", path);
	}
}

static void
service_recover_of(Id* id)
{
	auto file = service_file_allocate();
	defer(service_file_free, file);

	// read service file
	service_file_open(file, id);

	// if any of the output files are missing then remove the rest of them
	// (incomplete files will be removed during tier/part mgr recovery)
	//
	// case: crash after service file sync/rename
	//
	auto pos = file->output.start;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		Str path_relative;
		decode_basepath(&pos, &path_relative);

		// <base>/storage/<volume_id>/id
		if (fs_exists("%s/%.*s", state_directory(), str_size(&path_relative),
		              str_of(&path_relative)))
			continue;

		service_recover_gc(file->output.start);

		// remove service file
		id_delete(id, ID_SERVICE);
		return;
	}

	// output files considered valid, remove all remaining
	// input files
	//
	// case: crash after output files synced and renamed
	//
	service_recover_gc(file->input.start);

	// remove service file
	id_delete(id, ID_SERVICE);
}

void
service_recover(Service* self)
{
	unused(self);

	// <base>/storage
	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage", state_directory());

	// read directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("service: directory '%s' open error", path);
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// <id>.type[.incomplete]
		int64_t psn;
		auto state = id_of(entry->d_name, &psn);
		if (state == -1)
			continue;

		state_psn_follow(psn);

		Id id;
		id_init(&id);
		id.id     = psn;
		id.type   = state;
		id.volume = NULL;

		switch (state) {
		case ID_SERVICE_INCOMPLETE:
		{
			// remove incomplete service file
			id_delete(&id, state);
			break;
		}
		case ID_SERVICE:
		{
			// recover service file
			service_recover_of(&id);
			break;
		}
		default:
			info("service: unexpected file: '%s%s'", path,
			     entry->d_name);
			continue;
		}
	}
}
