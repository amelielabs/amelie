
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
	self->id = *id;
	meta_init(&self->meta);
	buf_init(&self->meta_data);
	file_init(&self->file);
	rbtree_init_node(&self->link_mapping);
	list_init(&self->link_volume);
	list_init(&self->link);
	return self;
}

void
object_free(Object* self)
{
	buf_free(&self->meta_data);
	file_close(&self->file);
	am_free(self);
}

static void
object_validate_index(Object* self, Buf* meta_data)
{
	uint32_t crc = runtime()->crc(0, meta_data->start, buf_size(meta_data));
	if (crc != self->meta.crc_data)
		error("object: file meta data '%s' crc mismatch",
		      str_of(&self->file.path));
}

static void
object_read_index(Object* self, size_t offset)
{
	auto file      = &self->file;
	auto meta      = &self->meta;
	auto meta_data = &self->meta_data;
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
		object_validate_index(self, meta_data);
		return;
	}

	// read index data
	auto buf = buf_create();
	defer_buf(buf);
	file_pread_buf(file, buf, meta->size, offset);
	object_validate_index(self, buf);

	// decompress index
	buf_emplace(meta_data, meta->size_origin);
	encoder_decode(&encoder, meta_data->start, buf_size(meta_data),
	               buf->start, buf_size(buf));
}

static void
object_read(Object* self)
{
	// [[meta] [regions] [meta_data]], ...
	uint64_t offset = 0;
	auto file = &self->file;
	if (unlikely((file->size - offset) < sizeof(Meta)))
		error("object: file '%s' has incorrect size",
		      str_of(&file->path));

	// read header
	auto meta = &self->meta;
	file_pread(file, meta, sizeof(Meta), offset);

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

	// set offset to the end of the object
	offset += meta->size_total;

	// validate file size according to the meta and the file type
	if (offset > file->size)
		error("object: file '%s' size mismatch",
		      str_of(&file->path));

	// read region index
	object_read_index(self, offset);
}

void
object_open(Object* self, int state)
{
	id_open(&self->id, &self->file, state);
	object_read(self);
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
object_status(Object* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id.id);

	// storage
	encode_raw(buf, "storage", 7);
	encode_string(buf, &self->id.volume->storage->config->name);

	// min
	encode_raw(buf, "min", 3);
	encode_integer(buf, 0);

	// max
	encode_raw(buf, "max", 3);
	encode_integer(buf, 0);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, 0);

	// size
	encode_raw(buf, "size", 4);
	encode_integer(buf, self->meta.size_total);

	// compression
	encode_raw(buf, "compression", 11);
	encode_integer(buf, self->meta.compression);

	encode_obj_end(buf);
}
