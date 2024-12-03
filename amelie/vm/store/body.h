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

typedef struct Body Body;

struct Body
{
	Buf*   buf;
	Local* local;
};

void body_init(Body*, Local*, Buf*);
void body_reset(Body*);
void body_write(Body*, Columns*, Value*);
void body_write_json(Body*, Buf*, bool);
void body_write_error(Body*, Error*);
