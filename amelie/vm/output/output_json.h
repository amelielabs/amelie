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

void output_json_result(Output*, Columns*, Value*);
void output_json_result_json(Output*, Str*, uint8_t*, bool);

extern OutputIf output_json;
