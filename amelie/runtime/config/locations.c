
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>

void
locations_init(Locations* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
locations_free(Locations* self)
{
	list_foreach_safe(&self->list)
	{
		auto remote = list_at(Remote, link);
		remote_free(remote);
	}
}

static inline Remote*
locations_read(uint8_t** pos)
{
	auto self = remote_allocate();
	guard(remote_free, self);
	Decode map[REMOTE_MAX + 1];
	for (int id = 0; id < REMOTE_MAX; id++)
	{
		map[id].flags = DECODE_STRING;
		map[id].key   = remote_nameof(id);
		map[id].value = remote_get(self, id);
	}
	memset(&map[REMOTE_MAX], 0, sizeof(Decode));
	decode_map(map, "remote", pos);
	return unguard();
}

void
locations_open(Locations* self, const char* path)
{
	if (! fs_exists("%s", path))
		return;

	Buf buf;
	buf_init(&buf);
	guard(buf_free, &buf);
	file_import(&buf, "%s", path);

	Str text;
	str_init(&text);
	buf_str(&buf, &text);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, global()->timezone, &text, NULL);

	// {}
	uint8_t* pos = json.buf->start;
	data_read_map(&pos);
	while (! data_read_map_end(&pos))
	{
		auto remote = locations_read(&pos);
		locations_add(self, remote);
	}
}

void
locations_sync(Locations* self, const char* path)
{
	if (fs_exists("%s", path))
		fs_unlink("%s", path);

	// write a list of remote locations
	Buf buf;
	buf_init(&buf);
	guard(buf_free, &buf);

	encode_map(&buf);
	list_foreach(&self->list)
	{
		auto remote = list_at(Remote, link);
		encode_string(&buf, remote_get(remote, REMOTE_NAME));
		encode_map(&buf);
		for (int id = 0; id < REMOTE_MAX; id++)
		{
			encode_cstr(&buf, remote_nameof(id));
			encode_string(&buf, remote_get(remote, id));
		}
		encode_map_end(&buf);
	}
	encode_map_end(&buf);

	// convert to json
	Buf text;
	buf_init(&text);
	guard(buf_free, &text);
	uint8_t* pos = buf.start;
	json_export_pretty(&text, NULL, &pos);

	// create file
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);

	// sync
	file_sync(&file);
}

void
locations_add(Locations* self, Remote* remote)
{
	list_append(&self->list, &remote->link);
	self->list_count++;
}

void
locations_delete(Locations* self, Str* name)
{
	auto remote = locations_find(self, name);
	if (! remote)
		return;
	list_unlink(&remote->link);
	self->list_count--;
	remote_free(remote);
}

Remote*
locations_find(Locations* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto remote = list_at(Remote, link);
		if (str_compare(remote_get(remote, REMOTE_NAME), name))
			return remote;
	}
	return NULL;
}

void
locations_set(Locations* self, Remote* remote, int argc, char** argv)
{
	for (int i = 0; i < argc; i++)
	{
		int id = 0;
		for (; id < REMOTE_MAX; id++)
		{
			auto arg = argv[i];
			if (! strncmp(arg, "--", 2))
				arg += 2;
			if (! strcasecmp(arg, remote_nameof(id)))
				break;
		}

		Str value;
		str_set_cstr(&value, argv[i]);
		if (id == REMOTE_MAX)
		{
			// not found, assuming argument is a remote name
			auto match = locations_find(self, &value);
			if (! match)
				error("remote: location or option '%s' not found", argv[i]);
			remote_copy(remote, match);
		} else
		{
			remote_set(remote, id, &value);
		}
	}
}
