
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
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
stream_mgr_init(StreamMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
stream_mgr_free(StreamMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
stream_mgr_create(StreamMgr*    self,
                  Tr*           tr,
                  StreamConfig* config,
                  bool          if_not_exists)
{
	// make sure stream does not exists
	auto stream = stream_mgr_find(self, &config->user, &config->name, false);
	if (stream)
	{
		if (! if_not_exists)
			error("stream '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create stream
	stream = stream_allocate(config);
	rel_mgr_create(&self->mgr, tr, &stream->rel);
	return true;
}

void
stream_mgr_drop_of(StreamMgr* self, Tr* tr, Stream* stream)
{
	// drop stream by object
	rel_mgr_drop(&self->mgr, tr, &stream->rel);
}

bool
stream_mgr_drop(StreamMgr* self, Tr* tr, Str* user, Str* name,
                bool       if_exists)
{
	auto stream = stream_mgr_find(self, user, name, false);
	if (! stream)
	{
		if (! if_exists)
			error("stream '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &stream->rel);

	stream_mgr_drop_of(self, tr, stream);
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

	auto stream = stream_of(op->rel);
	stream_config_set_user(stream->config, &user);
	stream_config_set_name(stream->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
stream_mgr_rename(StreamMgr* self,
                  Tr*        tr,
                  Str*       user,
                  Str*       name,
                  Str*       user_new,
                  Str*       name_new,
                  bool       if_exists)
{
	auto stream = stream_mgr_find(self, user, name, false);
	if (! stream)
	{
		if (! if_exists)
			error("stream '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &stream->rel);

	// ensure new stream does not exists
	if (stream_mgr_find(self, user_new, name_new, false))
		error("stream '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update stream
	log_rel(&tr->log, &rename_if, NULL, &stream->rel);

	// save previous name
	encode_string(&tr->log.data, &stream->config->user);
	encode_string(&tr->log.data, &stream->config->name);

	// set new name
	if (! str_compare_case(&stream->config->user, user_new))
		stream_config_set_user(stream->config, user_new);

	if (! str_compare_case(&stream->config->name, name_new))
		stream_config_set_name(stream->config, name_new);

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
	auto stream = stream_of(op->rel);
	auto grants = &stream->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
stream_mgr_grant(StreamMgr* self,
                 Tr*        tr,
                 Str*       user,
                 Str*       name,
                 Str*       to,
                 bool       grant,
                 uint32_t   perms,
                 bool       if_exists)
{
	auto stream = stream_mgr_find(self, user, name, false);
	if (! stream)
	{
		if (! if_exists)
			error("stream '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &stream->rel);

	// validate permissions
	auto perms_all = PERM_NONE;
	perms = permission_validate(user, name, perms, perms_all);

	// update stream
	log_rel(&tr->log, &grant_if, NULL, &stream->rel);

	// save previous grants
	auto grants = &stream->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

void
stream_mgr_dump(StreamMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto stream = stream_of(list_at(Rel, link));
		stream_config_write(stream->config, buf, 0);
	}
	encode_array_end(buf);
}

Stream*
stream_mgr_find(StreamMgr* self, Str* user, Str* name,
                bool       error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("stream '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return stream_of(rel);
}

Buf*
stream_mgr_list(StreamMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show stream
		auto stream = stream_mgr_find(self, user, name, false);
		if (stream)
			stream_config_write(stream->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show streames
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto stream = stream_of(list_at(Rel, link));
		if (user && !str_compare_case(&stream->config->user, user))
			continue;
		stream_config_write(stream->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
