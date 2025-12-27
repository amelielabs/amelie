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

typedef struct EncryptionIf EncryptionIf;
typedef struct Encryption   Encryption;

enum
{
	ENCRYPTION_NONE = 0,
	ENCRYPTION_AES  = 1
};

struct EncryptionIf
{
	int          id;
	Encryption* (*create)(EncryptionIf*);
	void        (*free)(Encryption*);
	void        (*encrypt_begin)(Encryption*, Random*, Str*, Buf*);
	void        (*encrypt_next)(Encryption*, Buf*, uint8_t*, int);
	void        (*encrypt_end)(Encryption*, Buf*);
	void        (*decrypt)(Encryption*, Str*, Buf*, uint8_t*, int);
};

struct Encryption
{
	EncryptionIf* iface;
};

static inline Encryption*
encryption_create(EncryptionIf* iface)
{
	return iface->create(iface);
}

static inline void
encryption_free(Encryption* self)
{
	return self->iface->free(self);
}

static inline void
encryption_begin(Encryption* self, Random* random, Str* key, Buf* buf)
{
	self->iface->encrypt_begin(self, random, key, buf);
}

static inline void
encryption_next(Encryption* self, Buf* buf, uint8_t* data, int data_size)
{
	self->iface->encrypt_next(self, buf, data, data_size);
}

static inline void
encryption_end(Encryption* self, Buf* buf)
{
	self->iface->encrypt_end(self, buf);
}

static inline void
encryption_decrypt(Encryption* self, Str* key,
                   Buf*        buf,
                   uint8_t*    data,
                   int         data_size)
{
	self->iface->decrypt(self, key, buf, data, data_size);
}
