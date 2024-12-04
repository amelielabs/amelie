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

typedef struct Content Content;

struct Content
{
	Buf*   content;
	Str    content_type;
	Local* local;
};

void content_init(Content*, Local*, Buf*);
void content_reset(Content*);
void content_write(Content*, Columns*, Value*);
void content_write_json(Content*, Buf*, bool);
void content_write_error(Content*, Error*);
