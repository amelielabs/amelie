
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

static bool
service_recover_validate(ServiceFile* self)
{
	// [[action, ...], ...]
	auto pos = self->actions.start;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		json_read_array(&pos);

		// action
		Str action;
		json_read_string(&pos, &action);

		bool valid = false;
		if (str_is(&action, "create", 6))
		{
			// [create, path]
			Str path;
			json_read_string(&pos, &path);

			// valid if created file exists

			// <base>/path
			valid = fs_exists("%s/%.*s", state_directory(),
			                  str_size(&path),
			                  str_of(&path));
		} else
		if (str_is(&action, "rename", 6))
		{
			// [rename, from, to]
			json_skip(&pos);
			Str path;
			json_read_string(&pos, &path);

			// valid if renamed file exists

			// <base>/path
			valid = fs_exists("%s/%.*s", state_directory(),
			                  str_size(&path),
			                  str_of(&path));
		} else {
			error("service: unknown action type");
		}

		json_read_array_end(&pos);
		if (! valid)
			return false;
	}
	return true;
}

static void
service_recover_abort(ServiceFile* self)
{
	// [[action, ...], ...]
	auto pos = self->actions.start;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		json_read_array(&pos);

		// action
		Str action;
		json_read_string(&pos, &action);

		if (str_is(&action, "create", 6))
		{
			// [create, path]
			Str path;
			json_read_string(&pos, &path);

			// remove file
			fs_unlink_if_exists("%s/%.*s", state_directory(),
			                    str_size(&path),
			                    str_of(&path));
		} else
		if (str_is(&action, "rename", 6))
		{
			// [rename, from, to]
			Str from;
			Str to;
			json_read_string(&pos, &from);
			json_read_string(&pos, &to);

			char path_from[PATH_MAX];
			sfmt(path_from, sizeof(path_from),
			     "%s/%.*s", state_directory(), str_size(&from),
			     str_of(&from));

			char path_to[PATH_MAX];
			sfmt(path_to, sizeof(path_to),
			     "%s/%.*s", state_directory(), str_size(&to),
			     str_of(&to));

			// rename file back, if it exists
			if (fs_exists("%s", path_to))
				fs_rename(path_to, "%s", path_from);

		} else {
			error("service: unknown action type");
		}

		json_read_array_end(&pos);
	}
}

static void
service_recover_resume(ServiceFile* self)
{
	// remove all existing origin files from the list

	// [path, ...]
	auto pos = self->origins.start;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		Str path_relative;
		decode_basepath(&pos, &path_relative);

		// <base>/path
		char path[PATH_MAX];
		sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
		     str_size(&path_relative),
		     str_of(&path_relative));

		if (fs_exists("%s", path))
			fs_unlink("%s", path);
	}
}

static void
service_recover_file(uint64_t id)
{
	auto file = service_file_allocate();
	defer(service_file_free, file);

	// read service file
	service_file_open(file, id);

	// if any of the actions are were not completed, rollback the rest of them
	// (incomplete files will be removed during tier/part mgr recovery)
	//
	// case: crash after service file sync/rename
	//
	if (! service_recover_validate(file))
	{
		// abort actions
		service_recover_abort(file);
	} else
	{
		// actions considered valid, remove all remaining origins
		//
		// case: crash after actions completed (synced and renamed)
		//
		service_recover_resume(file);
	}

	// remove service file
	service_file_delete(file);
}

void
service_recover(Service* self)
{
	unused(self);

	// <base>/storage
	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage", state_directory());

	// read storage directory
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

		// <id>.service[.incomplete]
		int64_t id = 0;
		auto ext = id_of_next(entry->d_name, &id);
		if  (!ext || *ext != '.')
			continue;

		// .service.incomplete
		if (! strcmp(ext, ".service.incomplete"))
		{
			// remove incomplete service file
			state_psn_follow(id);
			fs_unlink("%s/%s", path, entry->d_name);
			continue;
		}

		// .service
		if (! strcmp(ext, ".service"))
		{
			// recover service file
			state_psn_follow(id);
			service_recover_file(id);
		}
	}
}
