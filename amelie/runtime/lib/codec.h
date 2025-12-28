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

typedef struct CodecIf CodecIf;
typedef struct Codec   Codec;

struct CodecIf
{
	int     id;
	Codec* (*allocate)(CodecIf*);
	void   (*free)(Codec*);
	void   (*encode)(Codec*, Buf*, uint8_t*, int);
	void   (*encode_end)(Codec*, Buf*);
	void   (*decode)(Codec*, Buf*, uint8_t*, int);
};

struct Codec
{
	CodecIf* iface;
	List     link;
};

static inline Codec*
codec_allocate(CodecIf* iface)
{
	return iface->allocate(iface);
}

static inline void
codec_free(Codec* self)
{
	self->iface->free(self);
}

static inline void
codec_encode(Codec* self, Buf* buf, uint8_t* data, int data_size)
{
	self->iface->encode(self, buf, data, data_size);
}

static inline void
codec_encode_buf(Codec* self, Buf* buf, Buf* data)
{
	self->iface->encode(self, buf, data->start, buf_size(data));
}

static inline void
codec_encode_end(Codec* self, Buf* buf)
{
	self->iface->encode_end(self, buf);
}

static inline void
codec_decode(Codec* self, Buf* buf, uint8_t* data, int data_size)
{
	self->iface->decode(self, buf, data, data_size);
}
