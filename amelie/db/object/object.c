
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
#include <amelie_heap.h>
#include <amelie_object.h>

Object*
object_allocate(Storage* storage, Id* id, Encoding* encoding)
{
	auto self = (Object*)am_malloc(sizeof(Object));
	self->id       = *id;
	self->state    = ID_NONE;
	self->encoding = encoding;
	self->storage  = storage;
	meta_init(&self->meta);
	buf_init(&self->meta_data);
	file_init(&self->file);
	list_init(&self->link);
	return self;
}

void
object_free(Object* self)
{
	file_close(&self->file);
	buf_free(&self->meta_data);
	am_free(self);
}

static void
object_read(Object* self)
{
	// [regions] [[offsets][meta_regions]] [meta]
	auto file = &self->file;
	if (unlikely(file->size < sizeof(Meta)))
		error("object: file '%s' has incorrect size",
		      str_of(&file->path));

	// read footer
	int64_t offset = file->size - sizeof(Meta);
	auto meta = &self->meta;
	file_pread(file, meta, sizeof(Meta), offset);

	// check meta magic
	if (unlikely(meta->magic != META_MAGIC))
		error("object: file '%s' is corrupted",
		      str_of(&file->path));

	// check meta crc
	uint32_t crc;
	crc = runtime()->crc(0, (char*)meta + sizeof(uint32_t), sizeof(Meta) - sizeof(uint32_t));
	if (crc != meta->crc)
		error("object: file meta '%s' crc mismatch",
		      str_of(&file->path));

	// validate file size according to the meta and the file type
	auto valid = file->size == (meta->size_total);
	if (! valid)
		error("object: file '%s' size mismatch",
		      str_of(&file->path));

	// check meta format version
	if (unlikely(meta->version > META_VERSION))
		error("object: file '%s' version is not compatible",
		      str_of(&file->path));
}

static void
object_read_index_validate(Object* self, Buf* buf)
{
	uint32_t crc = runtime()->crc(0, buf->start, buf_size(buf));
	if (crc != self->meta.crc_data)
		error("object: file meta data '%s' crc mismatch",
		      str_of(&self->file.path));
}

static void
object_read_index(Object* self)
{
	// empty partition file
	auto meta = &self->meta;
	auto meta_data = &self->meta_data;
	if (meta->size == 0)
		return;

	auto file   = &self->file;
	auto offset = file->size - (sizeof(Meta) + meta->size);

	// prepare encoder
	Encoder encoder;
	encoder_init(&encoder);
	defer(encoder_free, &encoder);
	encoder_open(&encoder, self->encoding);
	encoder_set_encryption(&encoder, meta->encryption);
	encoder_set_compression(&encoder, meta->compression);

	// no compression or encryption
	if (! encoder_active(&encoder))
	{
		file_pread_buf(file, meta_data, meta->size, offset);
		object_read_index_validate(self, meta_data);
		return;
	}

	// read index data
	auto buf = buf_create();
	defer_buf(buf);
	file_pread_buf(file, buf, meta->size, offset);
	object_read_index_validate(self, buf);

	// decrypt and decompress index
	buf_emplace(meta_data, meta->size_origin);
	encoder_decode(&encoder, meta_data->start, buf_size(meta_data),
	               buf->start, buf_size(buf));
}

void
object_open(Object* self, int state, bool read_index)
{
	// <storage_path>/<id_tier>/<id_part>/<id>.lru
	// <storage_path>/<id_tier>/<id_part>/<id>.<parent_first>.<parent_last>

	// open object file
	storage_open(self->storage, &self->file, &self->id, state);

	// read meta data footer
	object_read(self);

	// optionally load index into memory
	if (read_index)
		object_read_index(self);
}

void
object_create(Object* self, int state)
{
	storage_create(self->storage, &self->file, &self->id, state);
}

void
object_delete(Object* self, int state)
{
	storage_delete(self->storage, &self->id, state);
}

void
object_rename(Object* self, int from, int to)
{
	storage_rename(self->storage, &self->id, from, to);
}
