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

typedef struct ObjectFile ObjectFile;

struct ObjectFile
{
	Id         id;
	File       file;
	atomic_u32 refs;
};

ObjectFile*
object_file_allocate(Id*);
void object_file_free(ObjectFile*);
void object_file_ref(ObjectFile*);
void object_file_unref(ObjectFile*, bool);
void object_file_create(ObjectFile*, int);
void object_file_open(ObjectFile*, int);
void object_file_delete(ObjectFile*, int);
void object_file_rename(ObjectFile*, int, int);
void object_file_read(ObjectFile*, Meta*, Buf*, size_t, bool);
