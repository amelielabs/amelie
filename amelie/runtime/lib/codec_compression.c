
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

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

typedef struct CodecZstd CodecZstd;

struct CodecZstd
{
	Codec      codec;
	ZSTD_CCtx* cctx;
	ZSTD_DCtx* dctx;
	int        level;
};

static Codec*
codec_zstd_allocate(CodecIf* iface)
{
	auto self = (CodecZstd*)am_malloc(sizeof(CodecZstd));
	self->codec.iface = iface;
	list_init(&self->codec.link);

	self->cctx = ZSTD_createCCtx();
	if (self->cctx == NULL)
	{
		am_free(self);
		error("zstd: failed to create codec context");
	}
	self->dctx = ZSTD_createDCtx();
	if (self->dctx == NULL)
	{
		ZSTD_freeCCtx(self->cctx);
		am_free(self);
		error("zstd: failed to create codec context");
	}
	return &self->codec;
}

static void
codec_zstd_free(Codec* codec)
{
	auto self = (CodecZstd*)codec;
	ZSTD_freeCCtx(self->cctx);
	ZSTD_freeDCtx(self->dctx);
	am_free(self);
}

static inline void
codec_zstd_prepare(Codec* codec, int level)
{
	auto self = (CodecZstd*)codec;
	self->level = level;
}

static inline void
codec_zstd_encode_begin(Codec* codec, Buf* buf)
{
	unused(buf);
	auto self = (CodecZstd*)codec;
	ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);
	if (self->level > 0)
		ZSTD_CCtx_setParameter(self->cctx, ZSTD_c_compressionLevel, self->level);
}

hot static void
codec_zstd_encode(Codec*   codec, Buf* buf,
                  uint8_t* data,
                  int      data_size)
{
	auto self = (CodecZstd*)codec;
	buf_reserve(buf, data_size);

	const auto min = ZSTD_CStreamOutSize();
	ZSTD_inBuffer input =
	{
		.src  = data,
		.size = data_size,
		.pos  = 0
	};
	while (input.pos < input.size)
	{
		buf_reserve(buf, min);

		ZSTD_outBuffer output =
		{
			.dst  = buf->position,
			.size = buf_size_unused(buf),
			.pos  = 0
		};
		auto rc = ZSTD_compressStream2(self->cctx, &output, &input, ZSTD_e_continue);
		if (ZSTD_isError(rc))
			error("zstd: encode failed: %s", ZSTD_getErrorName(rc));

		buf_advance(buf, output.pos);
	}
}

hot static void
codec_zstd_encode_end(Codec* codec, Buf* buf)
{
	auto self = (CodecZstd*)codec;

	const auto min = ZSTD_CStreamOutSize();
	ZSTD_inBuffer input = { NULL, 0, 0 };
	for (;;)
	{
		buf_reserve(buf, min);

		ZSTD_outBuffer output =
		{
			.dst  = buf->position,
			.size = buf_size_unused(buf),
			.pos  = 0
		};
		auto rc = ZSTD_compressStream2(self->cctx, &output, &input, ZSTD_e_end);
		if (ZSTD_isError(rc))
			error("zstd: encode failed: %s", ZSTD_getErrorName(rc));
		buf_advance(buf, output.pos);

		if (! rc)
			break;
	}
}

hot static void
codec_zstd_decode(Codec*   codec,
                  uint8_t* dst,
                  int      dst_size,
                  uint8_t* src,
                  int      src_size)
{
	auto self = (CodecZstd*)codec;
	ZSTD_DCtx_reset(self->dctx, ZSTD_reset_session_only);
	ZSTD_DCtx_setParameter(self->dctx, ZSTD_d_stableOutBuffer, 1);

	ZSTD_outBuffer output =
	{
		.dst  = dst,
		.size = dst_size,
		.pos  = 0
	};

	ZSTD_inBuffer input =
	{
		.src  = src,
		.size = src_size,
		.pos  = 0
	};

	auto rc = ZSTD_decompressStream(self->dctx, &output, &input);
	if (ZSTD_isError(rc))
		error("zstd: decode failed: %s", ZSTD_getErrorName(rc));
}

static CodecIf codec_zstd =
{
	.id           = COMPRESSION_ZSTD,
	.allocate     = codec_zstd_allocate,
	.free         = codec_zstd_free,
	.encode_begin = codec_zstd_encode_begin,
	.encode       = codec_zstd_encode,
	.encode_end   = codec_zstd_encode_end,
	.decode       = codec_zstd_decode
};

Codec*
compression_create(CodecCache* cache, int id, int level)
{
	// get codec iface
	CodecIf* iface = NULL;
	void   (*iface_prepare)(Codec*, int) = NULL;
	switch (id) {
	case COMPRESSION_ZSTD:
		iface = &codec_zstd;
		iface_prepare = codec_zstd_prepare;
		break;
	default:
		return NULL;
	}

	// get codec
	auto codec = codec_cache_pop(cache, id);
	if (! codec)
		codec = codec_allocate(iface);
	iface_prepare(codec, level);
	return codec;
}

int
compression_idof(Str* str)
{
	if (str_empty(str) || str_is(str, "none", 4))
		return COMPRESSION_NONE;
	if (str_is(str, "zstd", 4))
		return COMPRESSION_ZSTD;
	return -1;
}
