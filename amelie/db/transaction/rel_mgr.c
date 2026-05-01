
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
#include <amelie_row.h>
#include <amelie_transaction.h>

void
rel_mgr_init(RelMgr* self)
{
	hashtable_init(&self->ht);
	hashtable_init(&self->htid);
	self->list_count = 0;
	list_init(&self->list);
}

void
rel_mgr_free(RelMgr* self)
{
	hashtable_free(&self->ht);
	hashtable_free(&self->htid);

	list_foreach_safe(&self->list)
	{
		auto rel = list_at(Rel, link);
		rel_free(rel);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
rel_mgr_set_ht(RelMgr* self, Rel* rel)
{
	// add relation to the hash table
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);
	hashtable_reserve(&self->ht);

	// hash by name
	uint32_t hash = hash_murmur3_32(str_u8(rel->name), str_size(rel->name), 0);
	rel->link_ht.hash = hash;
	hashtable_set(&self->ht, &rel->link_ht);
}

static inline void
rel_mgr_set_htid(RelMgr* self, Rel* rel)
{
	// add relation to the hash table by id
	if (unlikely(! hashtable_created(&self->htid)))
		hashtable_create(&self->htid, 256);
	hashtable_reserve(&self->htid);

	// use uuid as a hash
	uint32_t hash = rel->id->a ^ rel->id->b;
	rel->link_htid.hash = hash;
	hashtable_set(&self->htid, &rel->link_htid);
}

static inline void
rel_mgr_set(RelMgr* self, Rel* rel)
{
	// previous version should not exists
	list_append(&self->list, &rel->link);
	self->list_count++;

	// add relation to the hash tables by name and id
	rel_mgr_set_ht(self, rel);
	if (rel->id)
		rel_mgr_set_htid(self, rel);
}

static inline void
rel_mgr_delete(RelMgr* self, Rel* rel)
{
	list_unlink(&rel->link);
	list_init(&rel->link);
	self->list_count--;

	hashtable_delete(&self->ht, &rel->link_ht);
	if (rel->id != NULL)
		hashtable_delete(&self->htid, &rel->link_htid);
}

void
rel_mgr_replace(RelMgr* self, Rel* prev, Rel* rel)
{
	rel_mgr_delete(self, prev);
	rel_mgr_set(self, rel);
}

static void
create_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
create_if_abort(Log* self, LogOp* op)
{
	unused(self);
	RelMgr* mgr = op->iface_arg;
	rel_mgr_delete(mgr, op->rel);
	rel_drop(op->rel);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

void
rel_mgr_create(RelMgr* self, Tr* tr, Rel* rel)
{
	// update rel mgr
	rel_mgr_set(self, rel);

	// update transaction log
	log_ddl(&tr->log, &create_if, self, rel);
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	unused(self);
	rel_drop(op->rel);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	RelMgr* mgr = op->iface_arg;
	rel_mgr_set(mgr, op->rel);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

void
rel_mgr_drop(RelMgr* self, Tr* tr, Rel* rel)
{
	// update rel mgr
	rel_mgr_delete(self, rel);

	// update transaction log
	log_ddl(&tr->log, &drop_if, self, rel);
}

void
rel_mgr_rename(RelMgr* self, Rel* rel, Str* user, Str* name)
{
	rel_mgr_delete(self, rel);

	// set new relation name
	//
	// note: this updates relation config directly
	//
	if (user && !str_compare(rel->user, user))
	{
		str_free(rel->user);
		str_copy(rel->user, user);
	}

	if (! str_compare(rel->name, name))
	{
		str_free(rel->name);
		str_copy(rel->name, name);
	}

	// update hash tables with new name
	//
	// (new name must be unique)
	//
	rel_mgr_set(self, rel);
}

void
rel_mgr_dump(RelMgr* self, RelType type, Buf* buf, int flags)
{
	// array
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != type)
			continue;
		rel_show(rel, buf, flags);
	}
	encode_array_end(buf);
}

void
rel_mgr_list(RelMgr* self, RelType type, Buf* buf, Str* user, Str* name, int flags)
{
	if (name)
	{
		// show <type>
		auto rel = rel_mgr_find(self, type, user, name, false);
		if (rel)
			rel_show(rel, buf, flags);
		else
			encode_null(buf);
		return;
	}

	// show <types>
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto rel = list_at(Rel, link);
		if (type != REL_UNDEF && rel->type != type)
			continue;
		if (user && !str_compare(rel->user, user))
			continue;
		rel_show(rel, buf, flags);
	}
	encode_array_end(buf);
}

void
rel_mgr_list_all(RelMgr* self, Buf* buf, Str* user, Str* name, int flags)
{
	if (name)
	{
		// show rel
		auto rel = rel_mgr_find(self, REL_UNDEF, user, name, false);
		if (rel)
			rel_write(rel, buf, flags);
		else
			encode_null(buf);
		return;
	}

	// show rels
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto rel = list_at(Rel, link);
		if (user && !str_compare(rel->user, user))
			continue;
		rel_write(rel, buf, flags);
	}
	encode_array_end(buf);
}


hot static inline bool
rel_mgr_cmp(Hashnode* node, void* ptr)
{
	// compare by name
	Str** arg = ptr;
	auto rel = container_of(node, Rel, link_ht);
	if (arg[0] && rel->user)
		if (! str_compare(rel->user, arg[0]))
			return false;
	return str_compare(rel->name, arg[1]);
}

hot static inline bool
rel_mgr_cmp_by(Hashnode* node, void* ptr)
{
	// compare by id
	Uuid* arg = ptr;
	auto rel = container_of(node, Rel, link_htid);
	if (! rel->id)
		return false;
	return uuid_is(rel->id, arg);
}

Rel*
rel_mgr_find(RelMgr* self, RelType type, Str* user, Str* name,
             bool    error_if_not_exists)
{
	// hash name
	uint32_t hash = hash_murmur3_32(str_u8(name), str_size(name), 0);

	// match relation
	Str* arg[] = { user, name };
	auto node = hashtable_get(&self->ht, hash, rel_mgr_cmp, arg);
	if (node)
	{
		auto rel = container_of(node, Rel, link_ht);
		if (type != REL_UNDEF && rel->type != type)
			return NULL;
		return rel;
	}

	if (error_if_not_exists)
		error("{s} '{str}': not exists", rel_type_of(type), name);
	return NULL;
}

Rel*
rel_mgr_find_by(RelMgr* self, RelType type, Uuid* id, bool error_if_not_exists)
{
	// hash by uuid
	uint32_t hash = id->a ^ id->b;

	// match relation
	auto node = hashtable_get(&self->htid, hash, rel_mgr_cmp_by, id);
	if (node)
	{
		auto rel = container_of(node, Rel, link_htid);
		if (type != REL_UNDEF && rel->type != type)
			return NULL;
		return rel;
	}

	if (error_if_not_exists)
	{
		char uuid[UUID_SZ];
		uuid_get(id, uuid, sizeof(uuid));
		error("{s} with uuid '{s}' not found", rel_type_of(type), uuid);
	}
	return NULL;
}
