
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>

void
timezones_init(Timezones* self)
{
	self->system = NULL;
	hashtable_init(&self->ht);
}

void
timezones_free(Timezones* self)
{
	if (self->ht.count > 0)
	{
		for (int i = 0; i < self->ht.size; i++)
		{
			auto node = hashtable_at(&self->ht, i);
			if (node == NULL)
				continue;
			auto tz = container_of(node, Timezone, node);
			timezone_free(tz);
		}
	}
	hashtable_free(&self->ht);
}

static void
timezones_read(Timezones* self, char* location, char* location_nested)
{
	char path[PATH_MAX];
	if (location_nested)
		format(path, sizeof(path), "{s}/{s}", location, location_nested);
	else
		format(path, sizeof(path), "{s}", location);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		return;
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		// nested timezone directory (like Europe/Moscow)
		if (entry->d_type == DT_DIR)
		{
			if (! location_nested)
				timezones_read(self, location, entry->d_name);
			continue;
		}

		// resize hash table if necessary
		hashtable_reserve(&self->ht);

		// read and register timezone file
		if (location_nested)
			format(path, sizeof(path), "{s}/{s}/{s}", location, location_nested, entry->d_name);
		else
			format(path, sizeof(path), "{s}/{s}", location, entry->d_name);

		char name_sz[PATH_MAX];
		if (location_nested)
			format(name_sz, sizeof(name_sz), "{s}/{s}", location_nested, entry->d_name);
		else
			format(name_sz, sizeof(name_sz), "{s}", entry->d_name);

		Str name;
		str_set_cstr(&name, name_sz);

		auto tz = timezone_create(&name, path);
		if (tz)
		{
			tz->node.hash = hash_fnv_lower(&name);
			hashtable_set(&self->ht, &tz->node);
		}
	}
}

#if 0
static void
timezones_read_system_timezone(Timezones* self)
{
	// read /etc/timezone content
	auto buf = file_import("/etc/timezone");
	defer(buf_free, buf);

	Str name;
	str_init(&name);
	buf_str(buf, &name);
	str_chomp(&name);

	// set as system timezone in timezone mgr
	self->system = timezones_find(self, &name);
	if (! self->system)
		error("timezone: failed to set system timezone '{str}'", &name);
}
#endif

static void
timezones_read_system_localtime(Timezones* self)
{
	char path[PATH_MAX];
	auto size = readlink("/etc/localtime", path, sizeof(path));
	if (size == -1)
		error("timezone: failed to read /etc/localtime symlink");

	// exclude directory name from the path
	if (size <= 20 || memcmp(path, "/usr/share/zoneinfo/", 20) != 0)
		error("timezone: /etc/localtime has invalid path");

	// set as system timezone in timezone mgr
	Str name;
	str_set(&name, path + 20, size - 20);
	self->system = timezones_find(self, &name);
	if (! self->system)
		error("timezone: failed to set system timezone '{str}'", &name);
}

void
timezones_open(Timezones* self)
{
	hashtable_create(&self->ht, 2048);
	timezones_read(self, "/usr/share/zoneinfo", NULL);
	timezones_read_system_localtime(self);
}

hot static inline bool
timezone_cmp(Hashnode* node, void* ptr)
{
	auto tz = container_of(node, Timezone, node);
	Str* with = ptr;
	return str_is_case(&tz->name, str_of(with), str_size(with));
}

hot Timezone*
timezones_find(Timezones* self, Str* name)
{
	uint32_t hash = hash_fnv_lower(name);
	auto node = hashtable_get(&self->ht, hash, timezone_cmp, name);
	if (! node)
		return NULL;
	return container_of(node, Timezone, node);
}
