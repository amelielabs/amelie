
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
	self->column      = -1;
	self->parts_count = 0;
	str_init(&self->user);
	str_init(&self->name);
	buf_init(&self->parts);
	return self;
}

static inline void
manifest_rel_free(ManifestRel* self)
{
	str_free(&self->user);
	str_free(&self->name);
	buf_free(&self->parts);
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
	uint8_t* parts = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,            "user",     &rel->user     },
		{ DECODE_STR,            "name",     &rel->name     },
		{ DECODE_INT|DECODE_OPT, "column",   &rel->column   },
		{ DECODE_ARRAY,          "parts",    &parts         },
		{ 0,                      NULL,       NULL          },
	};
	decode_obj(obj, "manifest", pos);

	// [[int, ...], ...]
	unpack_array(&parts);
	while (! unpack_array_end(&parts))
	{
		auto part_offset = buf_size(&rel->parts);
		auto part = (ManifestPart*)buf_emplace(&rel->parts, sizeof(ManifestPart));
		part->order = rel->parts_count;
		part->pages_count = 0;

		auto count = 0;
		unpack_array(&parts);
		while (! unpack_array_end(&parts))
		{
			int64_t* value = (int64_t*)buf_emplace(&rel->parts, sizeof(int64_t));
			unpack_int(&parts, value);
			if (*value > self->psn)
				self->psn = *value;
			count++;
		}

		part = (ManifestPart*)(rel->parts.start + part_offset);
		part->pages_count = count;

		rel->parts_count++;
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

static ManifestRel*
manifest_find_rel(Manifest* self, Str* user, Str* name, int column)
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

static ManifestPart*
manifest_find_part(ManifestRel* self, int order)
{
	if (! self->parts_count)
		return NULL;
	if (order >= self->parts_count)
		return NULL;
	auto pos = self->parts.start;
	auto end = self->parts.position;
	while (pos < end)
	{
		auto at = (ManifestPart*)pos;
		if (at->order == order)
			return at;
		pos += sizeof(ManifestPart) + sizeof(int64_t) * at->pages_count;
	}
	return NULL;
}

ManifestPart*
manifest_find(Manifest* self, Str* user, Str* name, int column, int part)
{
	auto ref = manifest_find_rel(self, user, name, column);
	if (! ref)
		return NULL;
	return manifest_find_part(ref, part);
}
