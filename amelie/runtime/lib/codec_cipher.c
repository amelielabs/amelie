
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

#include <openssl/crypto.h>
#include <openssl/evp.h>

typedef struct CodecAes CodecAes;

struct CodecAes
{
	Codec           codec;
	EVP_CIPHER_CTX* ctx;
	Str*            key;
	Random*         random;
};

static Codec*
codec_aes_allocate(CodecIf* iface)
{
	CodecAes* self = am_malloc(sizeof(CodecAes));
	self->codec.iface = iface;
	list_init(&self->codec.link);

	self->ctx = EVP_CIPHER_CTX_new();
	if (self->ctx == NULL)
	{
		am_free(self);
		error("aes: EVP_CIPHER_CTX_new() failed");
	}
	if (! EVP_CipherInit_ex2(self->ctx, EVP_aes_256_gcm(), NULL, NULL, true, NULL))
	{
		EVP_CIPHER_CTX_free(self->ctx);
		am_free(self);
		error("aes: EVP_CipherInit_ex2() failed");
	}
	self->key = NULL;
	return &self->codec;
}

static void
codec_aes_free(Codec* codec)
{
	auto self = (CodecAes*)codec;
	if (self->ctx)
		EVP_CIPHER_CTX_free(self->ctx);
	am_free(self);
}

hot static void
codec_aes_prepare(Codec* codec, Random* random, Str* key)
{
	auto self = (CodecAes*)codec;
	if (unlikely(str_size(key) != 32))
		error("aes: codec key must be 256bit");
	self->key    = key;
	self->random = random;
}

hot static void
codec_aes_encode_begin(Codec* codec, Buf* buf)
{
	auto self = (CodecAes*)codec;

	// [iv, tag, encodeed data]

	// calculate total size
	size_t size = 32 + EVP_MAX_BLOCK_LENGTH;
	buf_reserve(buf, size);
	buf_advance(buf, 32);

	// generate IV
	auto iv = (uint64_t*)buf->start;
	iv[0] = random_generate(self->random);
	iv[1] = random_generate(self->random);

	// prepare cipher
	if (! EVP_CipherInit_ex2(self->ctx, NULL, str_u8(self->key), (uint8_t*)iv, true, NULL))
		error("aes: EVP_CipherInit_ex2() failed");
}

hot static void
codec_aes_encode(Codec*   codec,
                 Buf*     buf,
                 uint8_t* data,
                 int      data_size)

{
	auto self = (CodecAes*)codec;
	buf_reserve(buf, data_size);
	int outlen = 0;
	if (! EVP_CipherUpdate(self->ctx, buf->position, &outlen, data, data_size))
		error("aes: EVP_CipherUpdate() failed");
	buf_advance(buf, outlen);
}

hot static void
codec_aes_encode_end(Codec* codec, Buf* buf)
{
	auto self = (CodecAes*)codec;

	// finilize
	int outlen = 0;
	if (! EVP_CipherFinal_ex(self->ctx, buf->position, &outlen))
		error("aes: EVP_CipherFinal_ex() failed");
	buf_advance(buf, outlen);

	// get tag and update tag
	EVP_CIPHER_CTX_ctrl(self->ctx, EVP_CTRL_GCM_GET_TAG, 16, buf->start + 16);
}

hot static void
codec_aes_decode(Codec*   codec,
                 uint8_t* dst,
                 int      dst_size,
                 uint8_t* src,
                 int      src_size)
{
	auto self = (CodecAes*)codec;
	if (unlikely(str_size(self->key) != 32))
		error("aes: codec key must be 256bit");

	// [iv, tag, encoded data]
	assert(dst_size >= src_size);
	uint8_t* iv  = src;
	uint8_t* tag = src + 16;
	src += 32;
	src_size -= 32;
	assert(src_size >= 0);

	// prepare cipher
	if (! EVP_CipherInit_ex2(self->ctx, NULL, str_u8(self->key), iv, false, NULL))
		error("aes: EVP_CipherInit_ex2() failed");

	// set tag
	EVP_CIPHER_CTX_ctrl(self->ctx, EVP_CTRL_GCM_SET_TAG, 16, tag);

	// decode
	int outlen = 0;
	if (! EVP_CipherUpdate(self->ctx, dst, &outlen, src, src_size))
		error("aes: EVP_CipherUpdate() failed");

	// finilize
	if (! EVP_CipherFinal_ex(self->ctx, dst + outlen, &outlen))
		error("aes: decodeion failed");
}

static CodecIf codec_aes =
{
	.id           = CIPHER_AES,
	.allocate     = codec_aes_allocate,
	.free         = codec_aes_free,
	.encode_begin = codec_aes_encode_begin,
	.encode       = codec_aes_encode,
	.encode_end   = codec_aes_encode_end,
	.decode       = codec_aes_decode
};

Codec*
cipher_create(CodecCache* cache, int id, Random* random, Str* key)
{
	// get codec iface
	CodecIf* iface = NULL;
	void   (*iface_prepare)(Codec*, Random*, Str*) = NULL;
	switch (id) {
	case CIPHER_AES:
		iface = &codec_aes;
		iface_prepare = codec_aes_prepare;
		break;
	default:
		return NULL;
	}

	// get codec
	auto codec = codec_cache_pop(cache, id);
	if (! codec)
		codec = codec_allocate(iface);
	iface_prepare(codec, random, key);
	return codec;
}

int
cipher_idof(Str* str)
{
	if (str_empty(str) || str_is(str, "none", 4))
		return CIPHER_NONE;
	if (str_is(str, "aes", 3))
		return CIPHER_AES;
	return -1;
}
