
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
sub_mgr_init(SubMgr* self, TableMgr* table_mgr, Cdc* cdc)
{
	self->table_mgr = table_mgr;
	self->cdc = cdc;
	rel_mgr_init(&self->mgr);
}

void
sub_mgr_free(SubMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
sub_mgr_create(SubMgr* self, Tr* tr, SubConfig* config, bool if_not_exists)
{
	auto sub = sub_mgr_find(self, &config->user, &config->name, false);
	if (sub)
	{
		if (! if_not_exists)
			error("subscription '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// ensure tables exists
	auto id  = (Uuid*)config->rels.start;
	auto end = (Uuid*)config->rels.position;
	for (; id < end; id++)
		table_mgr_find_by(self->table_mgr, id, true);

	// mark for cdc
	id = (Uuid*)config->rels.start;
	for (; id < end; id++)
	{
		auto table = table_mgr_find_by(self->table_mgr, id, true);
		table->part_arg.cdc++;
	}

	// allocate storage
	sub = sub_allocate(config, self->table_mgr, self->cdc);

	// set lsn and prepare slot
	cdc_slot_set(&sub->slot, state_lsn());

	// attach slot
	cdc_attach(self->cdc, &sub->slot);

	// register storage
	rel_mgr_create(&self->mgr, tr, &sub->rel);
	return true;
}

void
sub_mgr_drop_of(SubMgr* self, Tr* tr, Sub* sub)
{
	// drop sub by object
	rel_mgr_drop(&self->mgr, tr, &sub->rel);
}

bool
sub_mgr_drop(SubMgr* self, Tr* tr, Str* user, Str* name, bool if_exists)
{
	auto sub = sub_mgr_find(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	sub_mgr_drop_of(self, tr, sub);
	return true;
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str user;
	Str name;
	json_read_string(&pos, &user);
	json_read_string(&pos, &name);

	auto sub = sub_of(op->rel);
	sub_config_set_user(sub->config, &user);
	sub_config_set_name(sub->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
sub_mgr_rename(SubMgr* self,
               Tr*     tr,
               Str*    user,
               Str*    name,
               Str*    user_new,
               Str*    name_new,
               bool    if_exists)
{
	auto sub = sub_mgr_find(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	// ensure new sub does not exists
	if (sub_mgr_find(self, user_new, name_new, false))
		error("subscription '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update sub
	log_rel(&tr->log, &rename_if, NULL, &sub->rel);

	// save previous name
	encode_string(&tr->log.data, &sub->config->user);
	encode_string(&tr->log.data, &sub->config->name);

	// set new name
	if (! str_compare_case(&sub->config->user, user_new))
		sub_config_set_user(sub->config, user_new);

	if (! str_compare_case(&sub->config->name, name_new))
		sub_config_set_name(sub->config, name_new);

	return true;
}

static void
grant_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
grant_if_abort(Log* self, LogOp* op)
{
	// restore grants
	uint8_t* pos = log_data_of(self, op);
	auto sub = sub_of(op->rel);
	auto grants = &sub->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
sub_mgr_grant(SubMgr*  self,
              Tr*      tr,
              Str*     user,
              Str*     name,
              Str*     to,
              bool     grant,
              uint32_t perms,
              bool     if_exists)
{
	auto sub = sub_mgr_find(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &sub->rel);

	// validate permissions
	auto perms_all = PERM_NONE;
	perms = permission_validate(user, name, perms, perms_all);

	// update sub
	log_rel(&tr->log, &grant_if, NULL, &sub->rel);

	// save previous grants
	auto grants = &sub->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

Buf*
sub_mgr_list(SubMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show subscription
		auto sub = sub_mgr_find(self, user, name, false);
		if (sub)
			sub_config_write(sub->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show subscriptions
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto sub = sub_of(list_at(Rel, link));
		if (user && !str_compare_case(&sub->config->user, user))
			continue;
		sub_config_write(sub->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}

void
sub_mgr_dump(SubMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto sub = sub_of(list_at(Rel, link));
		sub_config_write(sub->config, buf, 0);
	}
	encode_array_end(buf);
}

Sub*
sub_mgr_find(SubMgr* self, Str* user, Str* name,
             bool    error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return sub_of(rel);
}
