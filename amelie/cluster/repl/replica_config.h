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

typedef struct ReplicaConfig ReplicaConfig;

struct ReplicaConfig
{
	Uuid   id;
	Remote remote;
};

static inline ReplicaConfig*
replica_config_allocate(void)
{
	ReplicaConfig* self;
	self = am_malloc(sizeof(*self));
	uuid_init(&self->id);
	remote_init(&self->remote);
	return self;
}

static inline void
replica_config_free(ReplicaConfig* self)
{
	remote_free(&self->remote);
	am_free(self);
}

static inline void
replica_config_set_id(ReplicaConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
replica_config_set_remote(ReplicaConfig* self, Remote* remote)
{
	remote_copy(&self->remote, remote);
}

static inline ReplicaConfig*
replica_config_copy(ReplicaConfig* self)
{
	auto copy = replica_config_allocate();
	replica_config_set_id(copy, &self->id);
	replica_config_set_remote(copy, &self->remote);
	return copy;
}

static inline ReplicaConfig*
replica_config_read(uint8_t** pos)
{
	auto self = replica_config_allocate();
	errdefer(replica_config_free, self);
	Decode obj[] =
	{
		{ DECODE_UUID,   "id",         &self->id                                   },
		{ DECODE_STRING, "uri",        remote_get(&self->remote, REMOTE_URI)       },
		{ DECODE_STRING, "tls_capath", remote_get(&self->remote, REMOTE_PATH_CA)   },
		{ DECODE_STRING, "tls_ca",     remote_get(&self->remote, REMOTE_FILE_CA)   },
		{ DECODE_STRING, "tls_cert",   remote_get(&self->remote, REMOTE_FILE_CERT) },
		{ DECODE_STRING, "tls_key",    remote_get(&self->remote, REMOTE_FILE_KEY)  },
		{ DECODE_STRING, "token",      remote_get(&self->remote, REMOTE_TOKEN)     },
		{ 0,              NULL,        NULL                                        },
	};
	decode_obj(obj, "replica", pos);
	return self;
}

static inline void
replica_config_write(ReplicaConfig* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// uri
	encode_raw(buf, "uri", 3);
	encode_string(buf, remote_get(&self->remote, REMOTE_URI));

	// tls_capath
	encode_raw(buf, "tls_capath", 10);
	encode_string(buf, remote_get(&self->remote, REMOTE_PATH_CA));

	// tls_ca
	encode_raw(buf, "tls_ca", 6);
	encode_string(buf, remote_get(&self->remote, REMOTE_FILE_CA));

	// tls_cert
	encode_raw(buf, "tls_cert", 8);
	encode_string(buf, remote_get(&self->remote, REMOTE_FILE_CERT));

	// tls_key
	encode_raw(buf, "tls_key", 7);
	encode_string(buf, remote_get(&self->remote, REMOTE_FILE_KEY));

	// token
	encode_raw(buf, "token", 5);
	encode_string(buf, remote_get(&self->remote, REMOTE_TOKEN));
	encode_obj_end(buf);
}
