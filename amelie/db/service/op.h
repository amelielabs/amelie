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

void service_refresh(Service*, Uuid*, uint64_t, Str*);
void service_checkpoint(Service*);
void service_gc(Service*);
void service_sync(Service*, uint64_t, bool);
bool service_step(Service*);
