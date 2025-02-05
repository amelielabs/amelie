
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

#include <amelie_private.h>

static Buf*
version_create(void)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	encode_obj_end(buf);
	return buf;
}

void
version_save(const char* path)
{
	auto buf = version_create();
	defer_buf(buf);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// create config file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, &text);
}

void
version_open(const char* path)
{
	// read version file
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	file_import(&buf, "%s", path);

	Str text;
	str_init(&text);
	buf_str(&buf, &text);

	// parse json
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);
	uint8_t* pos = json.buf->start;

	Str version;
	str_init(&version);
	Decode obj[] =
	{
		{ DECODE_STRING_READ, "version", &version },
		{ 0,                   NULL,      NULL    },
	};
	decode_obj(obj, "version", &pos);

	// compare versions
	auto version_current = &state()->version.string;
	if (! str_compare(&version, version_current))
		error("current version '%.*s' does not match repository version '%.*s'",
		      str_size(version_current),
		      str_of(version_current),
		      str_size(&version),
		      str_of(&version));
}
