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

void update_unset(Value*, uint8_t*, Str*);
void update_set(Value*, Timezone* tz, uint8_t*, Str*, Value*);
