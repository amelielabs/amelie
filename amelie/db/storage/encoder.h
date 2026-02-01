#pragma once

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

typedef struct Encoder Encoder;

struct Encoder
{
	Iov    iov;
	Codec* compression;
	Buf    compression_buf;
	Codec* encryption;
	Buf    encryption_buf;
	Tier*  tier;
};

static inline bool
encoder_active(Encoder* self)
{
	return self->encryption || self->compression;
}

static inline void
encoder_init(Encoder* self)
{
	self->compression = NULL;
	self->encryption  = NULL;
	self->tier        = NULL;
	iov_init(&self->iov);
	buf_init(&self->compression_buf);
	buf_init(&self->encryption_buf);
}

static inline void
encoder_reset(Encoder* self)
{
	iov_reset(&self->iov);
	buf_reset(&self->compression_buf);
	buf_reset(&self->encryption_buf);
}

static inline void
encoder_free(Encoder* self)
{
	self->tier = NULL;
	if (self->compression)
	{
		codec_cache_push(&runtime()->cache_compression, self->compression);
		self->compression = NULL;
	}
	if (self->encryption)
	{
		codec_cache_push(&runtime()->cache_cipher, self->encryption);
		self->encryption = NULL;
	}
	iov_free(&self->iov);
	buf_free(&self->compression_buf);
	buf_free(&self->encryption_buf);
}

static inline int
encoder_compression(Encoder* self)
{
	if (self->compression)
		return self->compression->iface->id;
	return COMPRESSION_NONE;
}

static inline int
encoder_encryption(Encoder* self)
{
	if (self->encryption)
		return self->encryption->iface->id;
	return CIPHER_NONE;
}

static inline void
encoder_set_compression(Encoder* self, int id)
{
	if (self->compression)
	{
		if (self->compression->iface->id == id)
			return;
		codec_cache_push(&runtime()->cache_compression, self->compression);
		self->compression = NULL;
	}

	// set compression context
	if (id == COMPRESSION_NONE)
		return;
	self->compression = compression_create(&runtime()->cache_compression, id, 0);
	if (! self->compression)
		error("object: invalid compression id '%d'", id);
}

static inline void
encoder_set_encryption(Encoder* self, int id)
{
	if (self->encryption)
	{
		if (self->encryption->iface->id == id)
			return;
		codec_cache_push(&runtime()->cache_cipher, self->encryption);
		self->encryption = NULL;
	}

	// set encryption context
	if (id == CIPHER_NONE)
		return;
	self->encryption = cipher_create(&runtime()->cache_cipher, id, &runtime()->random,
	                                 &self->tier->encryption_key);
	if (! self->encryption)
		error("object: invalid encryption id '%d'", id);
}

static inline void
encoder_open(Encoder* self, Tier* tier)
{
	self->tier = tier;

	// set compression context
	int id;
	if (! str_empty(&tier->compression))
	{
		auto name = &tier->compression;
		id = compression_idof(name);
		if (id == -1)
			error("invalid compression '%.*s'", str_size(name), str_of(name));
	} else {
		id = COMPRESSION_NONE;
	}
	encoder_set_compression(self, id);

	// set encryption context
	if (! str_empty(&tier->encryption))
	{
		auto name = &tier->compression;
		auto id = cipher_idof(name);
		if (id == -1)
			error("invalid encryption '%.*s'", str_size(name), str_of(name));
	} else {
		id = CIPHER_NONE;
	}
	encoder_set_encryption(self, id);
}

static inline struct iovec*
encoder_iov(Encoder* self)
{
	return iov_pointer(&self->iov);
}

static inline int
encoder_iov_count(Encoder* self)
{
	return self->iov.iov_count;
}

static inline uint32_t
encoder_iov_crc(Encoder* self)
{
	// calculate encoded iov crc
	auto iov = encoder_iov(self);
	uint32_t crc = 0;
	for (auto i = 0; i < self->iov.iov_count; i++)
		crc = runtime()->crc(crc, iov[i].iov_base, iov[i].iov_len);
	return crc;
}

static inline void
encoder_add(Encoder* self, uint8_t* pointer, int size)
{
	// add to encode
	iov_add(&self->iov, pointer, size);
}

static inline void
encoder_add_buf(Encoder* self, Buf* buf)
{
	iov_add_buf(&self->iov, buf);
}

static inline void
encoder_encode(Encoder* self)
{
	auto iov = encoder_iov(self);

	// compress
	if (self->compression)
	{
		auto codec = self->compression;
		auto buf   = &self->compression_buf;
		codec_encode_begin(codec, buf);
		for (auto i = 0; i < self->iov.iov_count; i++)
			codec_encode(codec, buf, iov[i].iov_base, iov[i].iov_len);
		codec_encode_end(codec, buf);

		iov_reset(&self->iov);
		iov_add_buf(&self->iov, buf);
	}

	// encrypt
	if (self->encryption)
	{
		auto codec = self->encryption;
		auto buf   = &self->encryption_buf;
		codec_encode_begin(codec, buf);
		for (auto i = 0; i < self->iov.iov_count; i++)
			codec_encode(codec, buf, iov[i].iov_base, iov[i].iov_len);
		codec_encode_end(codec, buf);

		iov_reset(&self->iov);
		iov_add_buf(&self->iov, buf);
	}
}

static inline void
encoder_decode(Encoder* self,
               uint8_t* dst,
               int      dst_size,
               uint8_t* src,
               int      src_size)
{
	// note: dst must be allocated to fit decoded data

	// decompress
	Codec* codec;
	if (self->compression && self->encryption)
	{
		// decrypt src (using internal buffer)
		auto buf = &self->encryption_buf;
		assert(dst_size >= src_size);
		buf_reset(buf);
		buf_reserve(buf, src_size);
		codec_decode(self->encryption, buf->start, buf_size(buf), src, src_size);
		src = buf->start;

		// decompress
		codec = self->compression;
	} else
	if (self->compression)
	{
		codec = self->compression;
	} else
	if (self->encryption)
	{
		codec = self->encryption;
	} else {
		abort();
	}

	// decode to dst
	codec_decode(codec, dst, dst_size, src, src_size);
}
