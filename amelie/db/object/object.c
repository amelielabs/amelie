
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

Object*
object_allocate(Id* id)
{
	auto self = (Object*)am_malloc(sizeof(Object));
	self->id             = *id;
	self->branches       = NULL;
	self->branches_count = 0;
	self->root           = NULL;
	file_init(&self->file);
	rbtree_init_node(&self->link_mapping);
	list_init(&self->link);
	return self;
}

void
object_free(Object* self)
{
	auto branch = self->branches;
	while (branch)
	{
		auto next = branch->next;
		branch_free(branch);
		branch = next;
	}
	file_close(&self->file);
	am_free(self);
}

static void
object_validate_index(Object* self, Branch* branch)
{
	uint32_t crc = runtime()->crc(0, branch->meta_data.start, buf_size(&branch->meta_data));
	if (crc != branch->meta.crc_data)
		error("object: file meta data '%s' crc mismatch",
		      str_of(&self->file.path));
}

static void
object_read_index(Object* self, Branch* branch, size_t offset)
{
	auto file      = &self->file;
	auto meta      = &branch->meta;
	auto meta_data = &branch->meta_data;
	offset -= meta->size;

	// prepare encoder
	Encoder encoder;
	encoder_init(&encoder);
	defer(encoder_free, &encoder);
	encoder_open(&encoder, self->id.volume->storage);
	encoder_set_compression(&encoder, meta->compression);

	// no compression
	if (! encoder_active(&encoder))
	{
		file_pread_buf(file, meta_data, meta->size, offset);
		object_validate_index(self, branch);
		return;
	}

	// read index data
	auto buf = buf_create();
	defer_buf(buf);
	file_pread_buf(file, buf, meta->size, offset);
	object_validate_index(self, branch);

	// decompress index
	buf_emplace(meta_data, meta->size_origin);
	encoder_decode(&encoder, meta_data->start, buf_size(meta_data),
	               buf->start, buf_size(buf));
}

static Branch*
object_read(Object* self, size_t* offset)
{
	// [[meta] [regions] [meta_data]], ...
	auto branch = branch_allocate(&self->id, &self->file);
	errdefer(branch_free, branch);

	auto file = &self->file;
	if (unlikely((file->size - *offset) < sizeof(Meta)))
		error("object: file '%s' has incorrect size",
		      str_of(&file->path));

	// read header
	auto meta = &branch->meta;
	file_pread(file, meta, sizeof(Meta), *offset);

	// check header magic
	if (unlikely(meta->magic != META_MAGIC))
		error("object: file '%s' is corrupted",
		      str_of(&file->path));

	// check header crc
	uint32_t crc;
	crc = runtime()->crc(0, (char*)meta + sizeof(uint32_t), sizeof(Meta) - sizeof(uint32_t));
	if (crc != meta->crc)
		error("object: file header '%s' crc mismatch",
		      str_of(&file->path));

	// check header version
	if (unlikely(meta->version > META_VERSION))
		error("object: file '%s' version is not compatible",
		      str_of(&file->path));

	// set offset to the end of the branch
	*offset += meta->size_total;

	// validate file size according to the meta and the file type
	if (*offset > file->size)
		error("object: file '%s' size mismatch",
		      str_of(&file->path));

	// read region index
	object_read_index(self, branch, *offset);
	return branch;
}

void
object_open(Object* self, int state)
{
	id_open(&self->id, &self->file, state);

	// read branches
	size_t offset = 0;
	while (self->branches_count < (int)self->id.version)
	{
		auto branch = object_read(self, &offset);
		object_add(self, branch);
	}

	// cut branches that are not represented in its versions id
	// (compaction crash)
	if (offset == self->file.size)
	{
		file_truncate(&self->file, offset);
		if (opt_int_of(&config()->storage_sync))
			file_sync(&self->file);
		info("object: file '%s' truncated to %d branches", str_of(&self->file.path),
		     self->branches_count);
	}
}

void
object_create(Object* self, int state)
{
	id_create(&self->id, &self->file, state);
}

void
object_delete(Object* self, int state)
{
	id_delete(&self->id, state);
}

void
object_rename(Object* self, int from, int to)
{
	id_rename(&self->id, from, to);
}

void
object_add(Object* self, Branch* branch)
{
	branch->next = self->branches;
	self->branches = branch;
	self->branches_count++;
	if (! self->root)
		self->root = branch;
}

void
object_status(Object* self, Buf* buf, int flags, Str* tier)
{
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id.id);

	// tier
	encode_raw(buf, "tier", 4);
	encode_string(buf, tier);

	// storage
	encode_raw(buf, "storage", 7);
	encode_string(buf, &self->id.volume->storage->config->name);

	// branches
	encode_raw(buf, "branches", 8);
	encode_array(buf);
	for (auto branch = self->branches; branch; branch = branch->next)
		branch_status(branch, buf, flags);
	encode_array_end(buf);

	encode_obj_end(buf);
}
