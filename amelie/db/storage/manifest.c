
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
#include <amelie_type.h>
#include <amelie_storage.h>

static inline ManifestRel*
manifest_rel_allocate(void)
{
	auto self = (ManifestRel*)am_malloc(sizeof(ManifestRel));
	self->column = -1;
	self->pages_count = 0;
	buf_init(&self->pages);
	str_init(&self->user);
	str_init(&self->name);
	return self;
}

static inline void
manifest_rel_free(ManifestRel* self)
{
	str_free(&self->user);
	str_free(&self->name);
	buf_free(&self->pages);
	am_free(self);
}

void
manifest_init(Manifest* self)
{
	self->psn        = 0;
	self->list_count = 0;
	buf_init(&self->list);
}

void
manifest_free(Manifest* self)
{
	auto pos = (ManifestRel**)self->list.start;
	auto end = (ManifestRel**)self->list.position;
	for (; pos < end; pos++)
		manifest_rel_free(*pos);
	buf_free(&self->list);
}

static void
manifest_read(Manifest* self, uint8_t** pos)
{
	auto rel = manifest_rel_allocate();
	buf_write(&self->list, &rel, sizeof(ManifestRel**));
	self->list_count++;

	// {}
	uint8_t* pages = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,            "user",     &rel->user     },
		{ DECODE_STR,            "name",     &rel->name     },
		{ DECODE_INT|DECODE_OPT, "column",   &rel->column   },
		{ DECODE_ARRAY,          "pages",    &pages         },
		{ 0,                      NULL,       NULL          },
	};
	decode_obj(obj, "manifest", pos);

	// [int, ...]
	unpack_array(&pages);
	while (! unpack_array_end(&pages))
	{
		int64_t* value = (int64_t*)buf_emplace(&rel->pages, sizeof(int64_t));
		unpack_int(&pages, value);
		if (*value > self->psn)
			self->psn = *value;
		rel->pages_count++;
	}
}

void
manifest_open(Manifest* self, char* path)
{
	auto buf = file_import("{s}", path);
	defer_buf(buf);
	Str text;
	buf_str(buf, &text);

	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);

	// []
	auto pos = json.buf->start;
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
		manifest_read(self, &pos);
}

ManifestRel*
manifest_find(Manifest* self, Str* user, Str* name, int column)
{
	auto pos = (ManifestRel**)self->list.start;
	auto end = (ManifestRel**)self->list.position;
	for (; pos < end; pos++)
	{
		auto at = *pos;
		if (at->column != column)
			continue;
		if (!str_compare(&at->user, user) ||
		    !str_compare(&at->name, name))
			continue;
		return at;
	}
	return NULL;
}
