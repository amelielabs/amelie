#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ClientMgr ClientMgr;

struct ClientMgr
{
	uint64_t seq;
	List     list;
	int      list_count;
};

static inline void
client_mgr_init(ClientMgr* self)
{
	self->seq   = 0;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
client_mgr_free(ClientMgr* self)
{
	assert(self->list_count == 0);
}

static inline void
client_mgr_add(ClientMgr* self, Client* client)
{
	client->id = ++self->seq;
	list_append(&self->list, &client->link);
	self->list_count++;
}

static inline void
client_mgr_del(ClientMgr* self, Client* client)
{
	list_unlink(&client->link);
	self->list_count--;
}

static inline Buf*
client_mgr_list(ClientMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);

	encode_map(buf, self->list_count);
	list_foreach(&self->list)
	{
		auto client = list_at(Client, link);

		// "id": { }
		char id[16];
		int  id_size = snprintf(id, sizeof(id), "%" PRIu64, client->id);
		encode_raw(buf, id, id_size);

		// map
		encode_map(buf, 1);

		// access
		encode_raw(buf, "access", 6);
		Str access;
		access_str(client->access, &access);
		encode_string(buf, &access);
	}

	msg_end(buf);
	return buf;
}
