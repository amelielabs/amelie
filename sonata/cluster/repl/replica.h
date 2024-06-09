#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Replica Replica;

struct Replica
{
	Streamer       streamer;
	WalSlot        wal_slot;
	ReplicaConfig* config;
	List           link;
};

static inline void
replica_free(Replica* self)
{
	if (self->config)
		replica_config_free(self->config);
	streamer_free(&self->streamer);
	so_free(self);
}

static inline Replica*
replica_allocate(ReplicaConfig* config, Wal* wal)
{
	auto self = (Replica*)so_malloc(sizeof(Replica));
	self->config = NULL;
	wal_slot_init(&self->wal_slot);
	streamer_init(&self->streamer, wal, &self->wal_slot);
	list_init(&self->link);
	guard(replica_free, self);
	self->config = replica_config_copy(config);
	return unguard();
}

static inline void
replica_start(Replica* self)
{
	streamer_start(&self->streamer, &self->config->id, &self->config->uri);
}

static inline void
replica_stop(Replica* self)
{
	streamer_stop(&self->streamer);
}

static inline void
replica_status(Replica* self, Buf* buf)
{
	encode_map(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->config->id);

	// uri
	encode_raw(buf, "uri", 3);
	encode_string(buf, &self->config->uri);

	// connected
	encode_raw(buf, "connected", 9);
	encode_bool(buf, atomic_u32_of(&self->streamer.connected));

	// lsn
	encode_raw(buf, "lsn", 3);
	uint64_t lsn = atomic_u64_of(&self->wal_slot.lsn);
	encode_integer(buf, lsn);

	// lag
	encode_raw(buf, "lag", 3);
	encode_integer(buf, config_lsn() - lsn);

	encode_map_end(buf);
}
