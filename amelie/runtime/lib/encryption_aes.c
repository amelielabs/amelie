
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

typedef struct EncryptionAes EncryptionAes;

struct EncryptionAes
{
	Encryption      id;
	EVP_CIPHER_CTX* ctx;
};

static Encryption*
encryption_aes_create(EncryptionIf* iface)
{
	EncryptionAes* self = am_malloc(sizeof(EncryptionAes));
	self->id.iface = iface;
	self->ctx      = EVP_CIPHER_CTX_new();
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
	return &self->id;
}

static void
encryption_aes_free(Encryption* ptr)
{
	auto self = (EncryptionAes*)ptr;
	if (self->ctx)
		EVP_CIPHER_CTX_free(self->ctx);
	am_free(self);
}

hot static void
encryption_aes_encrypt_begin(Encryption* ptr,
                             Random*     random,
                             Str*        key,
                             Buf*        buf)
{
	auto self = (EncryptionAes*)ptr;
	if (unlikely(str_size(key) != 32))
		error("aes: encryption key must be 256bit");

	// [iv, tag, encrypted data]

	// calculate total size
	size_t size = 32 + EVP_MAX_BLOCK_LENGTH;
	buf_reserve(buf, size);
	buf_advance(buf, 32);

	// generate IV
	auto iv = (uint64_t*)buf->start;
	iv[0] = random_generate(random);
	iv[1] = random_generate(random);

	// prepare cipher
	if (! EVP_CipherInit_ex2(self->ctx, NULL, str_u8(key), (uint8_t*)iv, true, NULL))
		error("aes: EVP_CipherInit_ex2() failed");
}

hot static void
encryption_aes_encrypt_add(Encryption* ptr,
                           Buf*        buf,
                           uint8_t*    data,
                           int         data_size)

{
	auto self = (EncryptionAes*)ptr;
	buf_reserve(buf, data_size);
	int outlen = 0;
	if (! EVP_CipherUpdate(self->ctx, buf->position, &outlen, data, data_size))
		error("aes: EVP_CipherUpdate() failed");
	buf_advance(buf, outlen);
}

hot static void
encryption_aes_encrypt_end(Encryption* ptr, Buf* buf)
{
	auto self = (EncryptionAes*)ptr;

	// finilize
	int outlen = 0;
	if (! EVP_CipherFinal_ex(self->ctx, buf->position, &outlen))
		error("aes: EVP_CipherFinal_ex() failed");
	buf_advance(buf, outlen);

	// get tag and update tag
	EVP_CIPHER_CTX_ctrl(self->ctx, EVP_CTRL_GCM_GET_TAG, 16, buf->start + 16);
}

hot static void
encryption_aes_decrypt(Encryption* ptr,
                       Str*        key,
                       Buf*        buf,
                       uint8_t*    data,
                       int         data_size)
{
	auto self = (EncryptionAes*)ptr;
	if (unlikely(str_size(key) != 32))
		error("aes: encryption key must be 256bit");

	buf_reserve(buf, data_size);

	// [iv, tag, encrypted data]
	uint8_t* iv  = data;
	uint8_t* tag = data + 16;
	data += 32;
	data_size -= 32;

	// prepare cipher
	if (! EVP_CipherInit_ex2(self->ctx, NULL, str_u8(key), iv, false, NULL))
		error("aes: EVP_CipherInit_ex2() failed");

	// set tag
	EVP_CIPHER_CTX_ctrl(self->ctx, EVP_CTRL_GCM_SET_TAG, 16, tag);

	// decrypt
	int outlen = 0;
	if (! EVP_CipherUpdate(self->ctx, buf->position, &outlen, data, data_size))
		error("aes: EVP_CipherUpdate() failed");
	buf_advance(buf, outlen);

	// finilize
	if (! EVP_CipherFinal_ex(self->ctx, buf->position, &outlen))
		error("aes: decryption failed");
	buf_advance(buf, outlen);
}

EncryptionIf encryption_aes =
{
	.id            = ENCRYPTION_AES,
	.create        = encryption_aes_create,
	.free          = encryption_aes_free,
	.encrypt_begin = encryption_aes_encrypt_begin,
	.encrypt_add   = encryption_aes_encrypt_add,
	.encrypt_end   = encryption_aes_encrypt_end,
	.decrypt       = encryption_aes_decrypt
};
