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

typedef struct CompressionIf CompressionIf;
typedef struct Compression   Compression;

enum
{
	COMPRESSION_NONE = 0,
	COMPRESSION_ZSTD = 1
};

struct CompressionIf
{
	int           id;
	Compression* (*create)(CompressionIf*);
	void         (*free)(Compression*);
	void         (*compress_begin)(Compression*, int);
	void         (*compress_next)(Compression*, Buf*, uint8_t*, int);
	void         (*compress_end)(Compression*, Buf*);
	void         (*decompress)(Compression*, Buf*, uint8_t*, int);
};

struct Compression
{
	CompressionIf* iface;
};

static inline Compression*
compression_create(CompressionIf* iface)
{
	return iface->create(iface);
}

static inline void
compression_free(Compression* self)
{
	return self->iface->free(self);
}

static inline void
compression_begin(Compression* self, int level)
{
	self->iface->compress_begin(self, level);
}

static inline void
compression_next(Compression* self, Buf* buf, uint8_t* data, int data_size)
{
	self->iface->compress_next(self, buf, data, data_size);
}

static inline void
compression_end(Compression* self, Buf* buf)
{
	self->iface->compress_end(self, buf);
}

static inline void
compression_decompress(Compression* self, Buf* buf,
                       uint8_t*     data,
                       int          data_size)
{
	self->iface->decompress(self, buf, data, data_size);
}
