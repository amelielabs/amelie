
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

static inline Remote*
locations_read(uint8_t** pos)
{
	auto self = remote_allocate();
	guard(remote_free, self);
	Decode map[] =
	{
		{ DECODE_STRING, "name",     remote_get(self, REMOTE_NAME)      },
		{ DECODE_STRING, "uri",      remote_get(self, REMOTE_URI)       },
		{ DECODE_STRING, "tls_ca",   remote_get(self, REMOTE_FILE_CA)   },
		{ DECODE_STRING, "tls_cert", remote_get(self, REMOTE_FILE_CERT) },
		{ DECODE_STRING, "tls_key",  remote_get(self, REMOTE_FILE_KEY)  },
		{ DECODE_STRING, "token",    remote_get(self, REMOTE_TOKEN)     },
		{ 0,              NULL,      NULL                               },
	};
	decode_map(map, "remote", pos);
	return unguard();
}

static inline void
locations_write(Remote* remote, Buf* buf)
{
	encode_map(buf);
	encode_raw(buf, "name", 4);
	encode_string(buf, remote_get(remote, REMOTE_NAME));
	encode_raw(buf, "uri", 3);
	encode_string(buf, remote_get(remote, REMOTE_URI));
	encode_raw(buf, "tls_ca", 6);
	encode_string(buf, remote_get(remote, REMOTE_FILE_CA));
	encode_raw(buf, "tls_cert", 8);
	encode_string(buf, remote_get(remote, REMOTE_FILE_CERT));
	encode_raw(buf, "tls_key", 7);
	encode_string(buf, remote_get(remote, REMOTE_FILE_KEY));
	encode_raw(buf, "token", 5);
	encode_string(buf, remote_get(remote, REMOTE_TOKEN));
	encode_map_end(buf);
}

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
		am_free(remote);
	}
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
		locations_write(remote, &buf);
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
	am_free(remote);
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
