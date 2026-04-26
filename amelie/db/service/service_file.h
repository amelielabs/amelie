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

typedef struct ServiceFile ServiceFile;

struct ServiceFile
{
	uint64_t id;
	Buf      origins;
	Buf      actions;
};

static inline ServiceFile*
service_file_allocate(void)
{
	auto self = (ServiceFile*)am_malloc(sizeof(ServiceFile));
	self->id = 0;
	buf_init(&self->origins);
	buf_init(&self->actions);
	return self;
}

static inline void
service_file_free(ServiceFile* self)
{
	buf_free(&self->origins);
	buf_free(&self->actions);
	am_free(self);
}

static inline void
service_file_reset(ServiceFile* self)
{
	self->id = 0;
	buf_reset(&self->origins);
	buf_reset(&self->actions);
}

static inline void
service_file_begin(ServiceFile* self)
{
	encode_array(&self->origins);
	encode_array(&self->actions);
}

static inline void
service_file_end(ServiceFile* self)
{
	encode_array_end(&self->origins);
	encode_array_end(&self->actions);
}

static inline void
service_file_add_origin(ServiceFile* self, Id* id, int state)
{
	id_encode_path(id, state, &self->origins);
}

static inline void
service_file_add_create(ServiceFile* self, Id* id, int state)
{
	// [create, path]
	encode_array(&self->actions);
	encode_raw(&self->actions, "create", 6);
	id_encode_path(id, state, &self->actions);
	encode_array_end(&self->actions);
}

static inline void
service_file_add_rename(ServiceFile* self, Id* a, int a_state, Id* b, int b_state)
{
	// [rename, from, to]
	encode_array(&self->actions);
	encode_raw(&self->actions, "rename", 6);
	id_encode_path(a, a_state, &self->actions);
	id_encode_path(b, b_state, &self->actions);
	encode_array_end(&self->actions);
}

static inline void
service_file_add_rename_version(ServiceFile* self,
                                uint64_t     psn,
                                Volume*      volume,
                                int          state_from,
                                int          state_to)
{
	Id id =
	{
		.id     = psn,
		.volume = volume
	};
	service_file_add_rename(self, &id, state_from, &id, state_to);
}

static inline void
service_file_create(ServiceFile* self)
{
	// prepare file data
	auto buf = buf_create();
	defer_buf(buf);

	// {}
	encode_obj(buf);

	// origins
	encode_raw(buf, "origins", 7);
	buf_write_buf(buf, &self->origins);

	// actions
	encode_raw(buf, "actions", 7);
	buf_write_buf(buf, &self->actions);
	encode_obj_end(buf);

	// convert data to json text
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	auto pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// set id
	self->id = state_psn_next();

	// create <id>.service.incomplete
	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%" PRIu64 ".service.incomplete",
	     state_directory(), self->id);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_create(&file, path);
	file_write_buf(&file, &text);

	// sync incomplete service file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	file_close(&file);

	// rename as completed
	fs_rename(path, "%s/storage/%" PRIu64 ".service",
	          state_directory(), self->id);
}

static inline void
service_file_delete(ServiceFile* self)
{
	fs_unlink("%s/storage/%" PRIu64 ".service",
	          state_directory(), self->id);
}

static inline void
service_file_open(ServiceFile* self, uint64_t id)
{
	self->id = id;

	// open <id>.service file
	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%" PRIu64 ".service",
	     state_directory(), self->id);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);

	// read text
	auto buf = buf_create();
	defer_buf(buf);
	file_pread_buf(&file, buf, file.size, 0);
	Str text;
	buf_str(buf, &text);

	// convert json text into data
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, NULL);
	auto pos = json.buf->start;

	// decode service file
	uint8_t* pos_origins;
	uint8_t* pos_actions;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "origins", &pos_origins },
		{ DECODE_ARRAY, "actions", &pos_actions },
		{ 0,             NULL,     NULL         },
	};
	decode_obj(obj, "service", &pos);

	// write origins
	auto end = pos_origins;
	data_skip(&end);
	buf_write(&self->origins, pos_origins, end - pos_origins);

	// write actions
	end = pos_actions;
	data_skip(&end);
	buf_write(&self->actions, pos_actions, end - pos_actions);
}
