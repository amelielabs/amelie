
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
#include <amelie_heap.h>
#include <amelie_object.h>

void
object_file_open(File*   self,
                 Source* source,
                 Id*     id,
                 int     state,
                 Meta*   meta)
{
	// open partition file and read meta data footer

	// <source_path>/<table_uuid>/<id_parent>.<id>
	char path[PATH_MAX];
	id_path(id, source, state, path);
	file_open(self, path);

	if (unlikely(self->size < sizeof(Meta)))
		error("partition: file '%s' has incorrect size",
		      str_of(&self->path));

	// [regions][meta_data][meta]
	
	// read meta
	int64_t offset = self->size - sizeof(Meta);
	file_pread(self, meta, sizeof(Meta), offset);

	// check meta magic
	if (unlikely(meta->magic != META_MAGIC))
		error("partition: file '%s' is corrupted", str_of(&self->path));

	// check meta crc
	uint32_t crc;
	crc = runtime()->crc(0, (char*)meta + sizeof(uint32_t), sizeof(Meta) - sizeof(uint32_t));
	if (crc != meta->crc)
		error("partition: file meta '%s' crc mismatch",
		      str_of(&self->path));

	// validate file size according to the meta and the file type
	bool valid;
	valid = self->size == (meta->size_total);
	if (! valid)
		error("partition: file '%s' size mismatch",
		      str_of(&self->path));

	// check meta format version
	if (unlikely(meta->version > META_VERSION))
		error("partition: file '%s' version is not compatible",
		      str_of(&self->path));
}

static inline void
push_cipher(Codec* self)
{
	codec_cache_push(&runtime()->cache_cipher, self);
}

static inline void
push_compression(Codec* self)
{
	codec_cache_push(&runtime()->cache_compression, self);
}

static Buf*
meta_decrypt(File*   self,
             Source* source,
             Meta*   meta,
             Buf*    origin,
             Buf*    buf)
{
	if (meta->encryption == CIPHER_NONE)
		return origin;

	// get cipher
	auto codec = cipher_create(&runtime()->cache_cipher, meta->encryption,
	                           &runtime()->random,
	                           &source->encryption_key);
	if (! codec)
		error("partition: file '%s' unknown encryption id: %d",
		      str_of(&self->path), meta->encryption);
	defer(push_cipher, codec);

	// decrypt
	codec_decode(codec, buf, origin->start, buf_size(origin));
	return buf;
}

static Buf*
meta_decompress(File*   self,
                Source* source,
                Meta*   meta,
                Buf*    origin,
                Buf*    buf)
{
	unused(source);
	if (meta->compression == COMPRESSION_NONE)
		return origin;

	// get compression
	auto codec = compression_create(&runtime()->cache_compression,
	                                meta->compression, 0);
	if (! codec)
		error("partition: file '%s' unknown compression id: %d",
		      str_of(&self->path), meta->compression);
	defer(push_compression, codec);

	// decompress
	buf_reserve(buf, meta->size_origin);
	codec_decode(codec, buf, origin->start, buf_size(origin));
	return buf;
}

void
object_file_read(File*   self,
                 Source* source,
                 Meta*   meta,
                 Buf*    meta_data,
                 bool    copy)
{
	unused(source);

	// empty partition file
	if (meta->size == 0)
		return;

	// read meta data
	uint64_t offset = self->size - (sizeof(Meta) + meta->size);
	file_pread_buf(self, meta_data, meta->size, offset);

	// check data crc
	uint32_t crc = runtime()->crc(0, meta_data->start, buf_size(meta_data));
	if (crc != meta->crc_data)
		error("partition: file meta data '%s' crc mismatch",
		      str_of(&self->path));

	// copy meta data as is
	if (copy)
		return;

	auto origin = meta_data;

	// decrypt
	Buf decrypted;
	buf_init(&decrypted);
	defer_buf(&decrypted);
	origin = meta_decrypt(self, source, meta, origin, &decrypted);

	// decompress
	Buf decompressed;
	buf_init(&decompressed);
	defer_buf(&decompressed);
	origin = meta_decompress(self, source, meta, origin, &decompressed);

	// return
	if (origin != meta_data)
	{
		buf_reset(meta_data);
		buf_write(meta_data, origin->start, buf_size(origin));
	}
}

void
object_file_create(File* self, Source* source, Id* id, int state)
{
	char path[PATH_MAX];
	switch (state) {
	// <source_path>/<table_uuid>/<id_parent>.<id>
	case ID:
	{
		id_path(id, source, state, path);
		file_create(self, path);
		break;
	}
	default:
		abort();
		break;
	}
}

void
object_file_delete(Source* source, Id* id, int state)
{
	// <source_path>/<table_uuid>/<id_parent>.<id>
	char path[PATH_MAX];
	id_path(id, source, state, path);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

void
object_file_rename(Source* source, Id* id, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(id, source, from, path_from);
	id_path(id, source, to, path_to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
