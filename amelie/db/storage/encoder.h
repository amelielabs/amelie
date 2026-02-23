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
	Iov      iov;
	Codec*   compression;
	Buf      compression_buf;
	Storage* storage;
};

static inline bool
encoder_active(Encoder* self)
{
	return self->compression;
}

static inline void
encoder_init(Encoder* self)
{
	self->compression = NULL;
	self->storage     = NULL;
	iov_init(&self->iov);
	buf_init(&self->compression_buf);
}

static inline void
encoder_reset(Encoder* self)
{
	iov_reset(&self->iov);
	buf_reset(&self->compression_buf);
}

static inline void
encoder_free(Encoder* self)
{
	self->storage = NULL;
	if (self->compression)
	{
		codec_cache_push(&runtime()->cache_compression, self->compression);
		self->compression = NULL;
	}
	iov_free(&self->iov);
	buf_free(&self->compression_buf);
}

static inline int
encoder_compression(Encoder* self)
{
	if (self->compression)
		return self->compression->iface->id;
	return COMPRESSION_NONE;
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
encoder_open(Encoder* self, Storage* storage)
{
	self->storage = storage;

	// set compression context
	int id;
	if (! str_empty(&storage->config->compression))
	{
		auto name = &storage->config->compression;
		id = compression_idof(name);
		if (id == -1)
			error("invalid compression '%.*s'", str_size(name), str_of(name));
	} else {
		id = COMPRESSION_NONE;
	}
	encoder_set_compression(self, id);
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
	if (self->compression)
	{
		codec = self->compression;
	} else {
		abort();
	}

	// decode to dst
	codec_decode(codec, dst, dst_size, src, src_size);
}
