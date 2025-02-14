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

typedef struct Replica Replica;

struct Replica
{
	Streamer       streamer;
	WalSlot        wal_slot;
	ReplicaConfig* config;
	List           link;
};

static inline Replica*
replica_allocate(ReplicaConfig* config, Wal* wal)
{
	auto self = (Replica*)am_malloc(sizeof(Replica));
	self->config = replica_config_copy(config);
	wal_slot_init(&self->wal_slot);
	streamer_init(&self->streamer, wal, &self->wal_slot);
	list_init(&self->link);
	return self;
}

static inline void
replica_free(Replica* self)
{
	if (self->config)
		replica_config_free(self->config);
	streamer_free(&self->streamer);
	am_free(self);
}

static inline void
replica_start(Replica* self)
{
	streamer_start(&self->streamer, &self->config->id, &self->config->remote);
}

static inline void
replica_stop(Replica* self)
{
	streamer_stop(&self->streamer);
}

static inline void
replica_status(Replica* self, Buf* buf)
{
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->config->id);

	// uri
	encode_raw(buf, "uri", 3);
	encode_string(buf, remote_get(&self->config->remote, REMOTE_URI));

	// connected
	encode_raw(buf, "connected", 9);
	encode_bool(buf, atomic_u32_of(&self->streamer.connected));

	// lsn
	encode_raw(buf, "lsn", 3);
	uint64_t lsn = atomic_u64_of(&self->wal_slot.lsn);
	encode_integer(buf, lsn);

	// lag
	encode_raw(buf, "lag", 3);
	encode_integer(buf, state_lsn() - lsn);

	encode_obj_end(buf);
}
