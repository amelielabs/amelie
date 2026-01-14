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

void object_file_open(File*, Source*, Id*, int, Meta*);
void object_file_read(File*, Source*, Meta*, Buf*, bool);
void object_file_create(File*, Source*, Id*, int);
void object_file_delete(Source*, Id*, int);
void object_file_rename(Source*, Id*, int, int);
