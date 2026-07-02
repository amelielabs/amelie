
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

#include <amelie>
#include <amelie_main.h>

void
bookmarks_init(Bookmarks* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
bookmarks_free(Bookmarks* self)
{
	list_foreach_safe(&self->list)
	{
		auto bookmark = list_at(Bookmark, link);
		bookmark_free(bookmark);
	}
}

void
bookmarks_open(Bookmarks* self, const char* path)
{
	if (! fs_exists("{s}", path))
		return;

	auto buf = file_import("{s}", path);
	defer_buf(buf);

	Str text;
	str_init(&text);
	buf_str(buf, &text);

	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);

	// {}
	uint8_t* pos = json.buf->start;
	unpack_obj(&pos);
	while (! unpack_obj_end(&pos))
	{
		// name
		data_skip(&pos);

		// value
		auto bookmark = bookmark_read(&pos);
		bookmarks_add(self, bookmark);
	}
}

void
bookmarks_sync(Bookmarks* self, const char* path)
{
	if (fs_exists("{s}", path))
		fs_unlink("{s}", path);

	// write a list of remote bookmarks
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// {}
	encode_obj(&buf);
	list_foreach(&self->list)
	{
		auto bookmark = list_at(Bookmark, link);
		encode_str(&buf, opt_string_of(&bookmark->endpoint.name));
		bookmark_write(bookmark, &buf);
	}
	encode_obj_end(&buf);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = buf.start;
	json_export_pretty(&text, NULL, &pos);

	// create file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);
}

void
bookmarks_add(Bookmarks* self, Bookmark* remote)
{
	list_append(&self->list, &remote->link);
	self->list_count++;
}

void
bookmarks_delete(Bookmarks* self, Str* name)
{
	auto remote = bookmarks_find(self, name);
	if (! remote)
		return;
	list_unlink(&remote->link);
	self->list_count--;
	bookmark_free(remote);
}

Bookmark*
bookmarks_find(Bookmarks* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto bookmark = list_at(Bookmark, link);
		if (str_compare(opt_string_of(&bookmark->endpoint.name), name))
			return bookmark;
	}
	return NULL;
}
