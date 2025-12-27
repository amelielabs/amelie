
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <zstd.h>

typedef struct CompressionZstd CompressionZstd;

struct CompressionZstd
{
	Compression id;
	ZSTD_CCtx*  ctx;
};

static Compression*
compression_zstd_create(CompressionIf* iface)
{
	CompressionZstd* self = am_malloc(sizeof(CompressionZstd));
	self->id.iface = iface;
	self->ctx      = ZSTD_createCCtx();
	if (self->ctx == NULL)
	{
		am_free(self);
		error("zstd: failed to create compression context");
	}
	return &self->id;
}

static void
compression_zstd_free(Compression* ptr)
{
	auto self = (CompressionZstd*)ptr;
	if (self->ctx)
		ZSTD_freeCCtx(self->ctx);
	am_free(self);
}

static void
compression_zstd_compress_begin(Compression* ptr, int level)
{
	auto self = (CompressionZstd*)ptr;
	ZSTD_CCtx_reset(self->ctx, ZSTD_reset_session_only);
	if (level > 0)
		ZSTD_CCtx_setParameter(self->ctx, ZSTD_c_compressionLevel, level);
}

hot static void
compression_zstd_compress_add(Compression* ptr, Buf* buf,
                              uint8_t*     data,
                              int          data_size)
{
	auto self = (CompressionZstd*)ptr;
	buf_reserve(buf, data_size);

	ZSTD_outBuffer out;
	out.dst  = buf->position;
	out.size = buf_size_unused(buf);
	out.pos  = 0;

	ZSTD_inBuffer in;
	in.src   = data;
	in.size  = data_size;
	in.pos   = 0;

	auto rc = ZSTD_compressStream2(self->ctx, &out, &in, ZSTD_e_continue);
	assert(rc == 0);
	unused(rc);

	buf_advance(buf, out.pos);
}

hot static void
compression_zstd_compress_end(Compression* ptr, Buf* buf)
{
	auto self = (CompressionZstd*)ptr;

	ZSTD_outBuffer out;
	out.dst  = buf->position;
	out.size = buf_size_unused(buf);
	out.pos  = 0;

	ZSTD_inBuffer in;
	memset(&in, 0, sizeof(in));
	auto rc = ZSTD_compressStream2(self->ctx, &out, &in, ZSTD_e_end);
	unused(rc);

	buf_advance(buf, out.pos);
}

hot static void
compression_zstd_decompress(Compression* ptr, Buf* buf,
                            uint8_t*     data,
                            int          data_size)
{
	unused(ptr);
	int rc = ZSTD_decompress(data, data_size, buf->start, buf_size(buf));
	if (unlikely(ZSTD_isError(rc)))
		error("zstd: decompression failed");
	assert(rc == data_size);
}

CompressionIf compression_zstd =
{
	.id             = COMPRESSION_ZSTD,
	.create         = compression_zstd_create,
	.free           = compression_zstd_free,
	.compress_begin = compression_zstd_compress_begin,
	.compress_add   = compression_zstd_compress_add,
	.compress_end   = compression_zstd_compress_end,
	.decompress     = compression_zstd_decompress
};
