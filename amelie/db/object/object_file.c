
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

void
object_file_free(ObjectFile* self)
{
	object_file_unref(self, false);
}

ObjectFile*
object_file_allocate(Id* id)
{
	auto self = (ObjectFile*)am_malloc(sizeof(ObjectFile));
	self->refs = 1;
	id_init(&self->id);
	id_set_free(&self->id, (IdFree)object_file_free);
	id_copy(&self->id, id);
	file_init(&self->file);
	return self;
}

void
object_file_ref(ObjectFile* self)
{
	atomic_u32_inc(&self->refs);
}

void
object_file_unref(ObjectFile* self, bool drop)
{
	if (atomic_u32_dec(&self->refs) != 1)
		return;
	defer(am_free, self);
	file_close(&self->file);
	if (drop)
		object_file_delete(self, ID_OBJECT);
}

void
object_file_create(ObjectFile* self, int state)
{
	id_create(&self->id, &self->file, state);
}

void
object_file_open(ObjectFile* self, int state)
{
	id_open(&self->id, &self->file, state);
}

void
object_file_delete(ObjectFile* self, int state)
{
	id_delete(&self->id, state);
}

void
object_file_rename(ObjectFile* self, int from, int to)
{
	id_rename(&self->id, from, to);
}

static void
object_file_read_index_validate(ObjectFile* self, Meta* meta, Buf* meta_data)
{
	uint32_t crc = runtime()->crc(0, meta_data->start, buf_size(meta_data));
	if (crc != meta->crc_data)
		error("object: file meta data '%s' crc mismatch",
		      str_of(&self->file.path));
}

static void
object_file_read_index(ObjectFile* self,
                       Meta*       meta,
                       Buf*        meta_data,
                       size_t      offset)
{
	// empty partition file
	if (meta->size == 0)
		return;
	offset -= sizeof(Meta) + meta->size;

	// prepare encoder
	Encoder encoder;
	encoder_init(&encoder);
	defer(encoder_free, &encoder);
	encoder_open(&encoder, self->id.volume->storage);
	encoder_set_compression(&encoder, meta->compression);

	// no compression
	auto file = &self->file;
	if (! encoder_active(&encoder))
	{
		file_pread_buf(file, meta_data, meta->size, offset);
		object_file_read_index_validate(self, meta, meta_data);
		return;
	}

	// read index data
	auto buf = buf_create();
	defer_buf(buf);
	file_pread_buf(file, buf, meta->size, offset);
	object_file_read_index_validate(self, meta, meta_data);

	// decompress index
	buf_emplace(meta_data, meta->size_origin);
	encoder_decode(&encoder, meta_data->start, buf_size(meta_data),
	               buf->start, buf_size(buf));
}

void
object_file_read(ObjectFile* self,
                 Meta*       meta,
                 Buf*        meta_data,
                 size_t      offset,
                 bool        read_index)
{
	// reading from end

	// <- ... [regions] [[offsets][meta_regions]] [meta]
	auto file = &self->file;
	if (unlikely(offset < sizeof(Meta)))
		error("object: file '%s' has incorrect size",
		      str_of(&file->path));

	// read footer
	file_pread(file, meta, sizeof(Meta), offset - sizeof(Meta));

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

	// check meta format version
	if (unlikely(meta->version > META_VERSION))
		error("object: file '%s' version is not compatible",
		      str_of(&file->path));

	// validate file size according to the meta and the file type
	auto valid = offset >= meta->size_total;
	if (! valid)
		error("object: file '%s' size mismatch",
		      str_of(&file->path));

	// read region index
	if (read_index)
		object_file_read_index(self, meta, meta_data, offset);
}
