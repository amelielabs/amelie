
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

void
timezone_mgr_init(TimezoneMgr* self)
{
	self->system = NULL;
	hashtable_init(&self->ht);
}

void
timezone_mgr_free(TimezoneMgr* self)
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
timezone_mgr_read(TimezoneMgr* self, char* location, char* location_nested)
{
	char path[PATH_MAX];
	if (location_nested)
		snprintf(path, sizeof(path), "%s/%s", location, location_nested);
	else
		snprintf(path, sizeof(path), "%s", location);
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		return;
	defer(fs_opendir_defer, dir);
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
				timezone_mgr_read(self, location, entry->d_name);
			continue;
		}

		// resize hash table if necessary
		hashtable_reserve(&self->ht);

		// read and register timezone file
		if (location_nested)
			snprintf(path, sizeof(path), "%s/%s/%s", location, location_nested, entry->d_name);
		else
			snprintf(path, sizeof(path), "%s/%s", location, entry->d_name);

		char name_sz[256];
		if (location_nested)
			snprintf(name_sz, sizeof(name_sz), "%s/%s", location_nested, entry->d_name);
		else
			snprintf(name_sz, sizeof(name_sz), "%s", entry->d_name);
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

static void
timezone_mgr_read_system_timezone(TimezoneMgr* self)
{
	// read /etc/timezone content
	auto buf = file_import("/etc/timezone");
	defer(buf_free, buf);

	Str name;
	str_init(&name);
	buf_str(buf, &name);
	str_chomp(&name);

	// set as system timezone in timezone mgr
	self->system = timezone_mgr_find(self, &name);
	if (! self->system)
		error("timezone: failed to set system timezone '%.*s'",
		      str_size(&name), str_of(&name));
}

void
timezone_mgr_open(TimezoneMgr* self)
{
	hashtable_create(&self->ht, 2048);
	timezone_mgr_read(self, "/usr/share/zoneinfo", NULL);
	timezone_mgr_read_system_timezone(self);
}

hot static inline bool
timezone_cmp(HashtableNode* node, void* ptr)
{
	auto tz = container_of(node, Timezone, node);
	Str* with = ptr;
	return str_is_case(&tz->name, str_of(with), str_size(with));
}

hot Timezone*
timezone_mgr_find(TimezoneMgr* self, Str* name)
{
	uint32_t hash = hash_fnv_lower(name);
	auto node = hashtable_get(&self->ht, hash, timezone_cmp, name);
	if (! node)
		return NULL;
	return container_of(node, Timezone, node);
}
