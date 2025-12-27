
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_io.h>

void
span_open(File*   self,
          Source* source,
          Id*     id,
          int     type,
          Span*   span)
{
	// open partition file and read span footer

	// <source_path>/<uuid>/<psn>
	char path[PATH_MAX];
	id_path(id, source, type, path);
	file_open(self, path);

	if (unlikely(self->size < sizeof(Span)))
		error("partition: file '%s' has incorrect size",
		      str_of(&self->path));

	// [regions][span_data][span]
	
	// read span
	int64_t offset = self->size - sizeof(Span);
	file_pread(self, span, sizeof(Span), offset);

	// check span magic
	if (unlikely(span->magic != SPAN_MAGIC))
		error("partition: file '%s' is corrupted", str_of(&self->path));

	// check span crc
	uint32_t crc;
	crc = runtime()->crc(0, (char*)span + sizeof(uint32_t), sizeof(Span) - sizeof(uint32_t));
	if (crc != span->crc)
		error("partition: file span '%s' crc mismatch",
		      str_of(&self->path));

	// validate file size according to the span and the file type
	bool valid;
	valid = self->size == (span->size_total);
	if (! valid)
		error("partition: file '%s' size mismatch",
		      str_of(&self->path));

	// check span format version
	if (unlikely(span->version > SPAN_VERSION))
		error("partition: file '%s' span format is not compatible",
		      str_of(&self->path));
}

static Buf*
span_decrypt(File*   self,
             Source* source,
             Span*   span,
             Buf*    origin,
             Buf*    buf)
{
	if (span->encryption == ENCRYPTION_NONE)
		return origin;

	if (span->encryption != ENCRYPTION_AES)
		error("partition: file '%s' unknown encryption: %d",
		      str_of(&self->path), span->encryption);

	// create encryption context
	auto context = encryption_create(&encryption_aes);
	defer(encryption_free, context);

	// decrypt
	encryption_decrypt(context,
	                   &source->encryption_key,
	                   buf,
	                   origin->start,
	                   buf_size(origin));
	return buf;
}

static Buf*
span_decompress(File*   self,
                Source* source,
                Span*   span,
                Buf*    origin,
                Buf*    buf)
{
	unused(source);
	if (span->compression == COMPRESSION_NONE)
		return origin;

	if (span->compression != COMPRESSION_ZSTD)
		error("partition: file '%s' unknown compression id: %d",
		      str_of(&self->path), span->compression);

	// create compression context
	auto context = compression_create(&compression_zstd);
	defer(compression_free, context);

	// decompress
	buf_reserve(buf, span->size_origin);
	compression_decompress(context, buf, origin->start,
	                       buf_size(origin));
	return buf;
}

void
span_read(File*   self,
          Source* source,
          Span*   span,
          Buf*    span_data,
          bool    copy)
{
	unused(source);

	// empty partition file
	if (span->size == 0)
		return;

	// read span data
	uint64_t offset = self->size - (sizeof(Span) + span->size);
	file_pread_buf(self, span_data, span->size, offset);

	// check data crc
	uint32_t crc = runtime()->crc(0, span_data->start, buf_size(span_data));
	if (crc != span->crc_data)
		error("partition: file span data '%s' crc mismatch",
		      str_of(&self->path));

	// copy span data as is
	if (copy)
		return;

	auto origin = span_data;

	// decrypt
	Buf buf_decrypted;
	buf_init(&buf_decrypted);
	defer_buf(&buf_decrypted);
	origin = span_decrypt(self, source, span, origin, &buf_decrypted);

	// decompress
	Buf buf_decompressed;
	buf_init(&buf_decompressed);
	defer_buf(&buf_decompressed);
	origin = span_decompress(self, source, span, origin, &buf_decompressed);

	// return
	if (origin != span_data)
	{
		buf_reset(span_data);
		buf_write(span_data, origin->start, buf_size(origin));
	}
}
