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

typedef struct Service Service;

struct Service
{
	Id  id;
	Buf input;
	Buf output;
};

static inline void
service_free(Service* self)
{
	buf_free(&self->input);
	buf_free(&self->output);
	am_free(self);
}

static inline Service*
service_allocate(void)
{
	auto self = (Service*)am_malloc(sizeof(Service));
	id_init(&self->id);
	id_set_free(&self->id, (IdFree)service_free);
	buf_init(&self->input);
	buf_init(&self->output);
	return self;
}

static inline void
service_reset(Service* self)
{
	buf_reset(&self->input);
	buf_reset(&self->output);
}

static inline void
service_begin(Service* self)
{
	encode_array(&self->input);
	encode_array(&self->output);
}

static inline void
service_add_input(Service* self, Id* id)
{
	id_encode_path(id, id->type, &self->input);
}

static inline void
service_add_output(Service* self, Id* id)
{
	id_encode_path(id, id->type, &self->output);
}

static inline void
service_end(Service* self)
{
	encode_array_end(&self->input);
	encode_array_end(&self->output);
}

static inline void
service_create(Service* self)
{
	// prepare file data
	auto buf = buf_create();
	defer_buf(buf);

	// {}
	encode_obj(buf);

	// input
	encode_raw(buf, "input", 5);
	buf_write_buf(buf, &self->input);

	// output
	encode_raw(buf, "output", 6);
	buf_write_buf(buf, &self->output);
	encode_obj_end(buf);

	// convert data to json text
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	auto pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// set id
	auto id = &self->id;
	id->id     = state_psn_next();
	id->type   = ID_SERVICE;
	id->volume = NULL;

	// create <id>.service.incomplete
	File file;
	file_init(&file);
	defer(file_close, &file);
	id_create(id, &file, ID_SERVICE_INCOMPLETE);
	file_write_buf(&file, &text);

	// sync incomplete service file
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	file_close(&file);

	// rename as completed
	id_rename(&self->id, ID_SERVICE_INCOMPLETE, ID_SERVICE);
}

static inline void
service_delete(Service* self)
{
	id_delete(&self->id, ID_SERVICE);
}

static inline void
service_open(Service* self, Id* id)
{
	// open <id>.service file
	File file;
	file_init(&file);
	defer(file_close, &file);
	id_open(id, &file, ID_SERVICE);

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
	uint8_t* pos_input;
	uint8_t* pos_output;
	Decode obj[] =
	{
		{ DECODE_ARRAY, "input",  &pos_input  },
		{ DECODE_ARRAY, "output", &pos_output },
		{ 0,             NULL,     NULL       },
	};
	decode_obj(obj, "service", &pos);

	// write input
	auto end = pos_input;
	json_skip(&end);
	buf_write(&self->input, pos_input, end - pos_input);

	// write output
	end = pos_output;
	json_skip(&end);
	buf_write(&self->output, pos_output, end - pos_output);
}
