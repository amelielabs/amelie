
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_pub.h>

void
pub_mgr_init(PubMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
pub_mgr_free(PubMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto pub = list_at(Pub, link);
		pub_free(pub);
	}
}

void
pub_mgr_add(PubMgr* self, Pub* pub)
{
	list_append(&self->list, &pub->link);
	self->list_count++;
}

void
pub_mgr_remove(PubMgr* self, Pub* pub)
{
	list_unlink(&pub->link);
	self->list_count--;
}

Pub*
pub_mgr_find(PubMgr* self, Uuid* id, bool error_if_not_exists)
{
	list_foreach(&self->list)
	{
		auto pub = list_at(Pub, link);
		if (uuid_is(&pub->id, id))
			return pub;
	}
	if (error_if_not_exists)
	{
		char uuid[UUID_SZ];
		uuid_get(id, uuid, sizeof(uuid));
		error("subscription '%s': not exists", uuid);
	}
	return NULL;
}
