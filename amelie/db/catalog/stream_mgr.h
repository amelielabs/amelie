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

typedef struct StreamMgr StreamMgr;

struct StreamMgr
{
	RelMgr mgr;
};

void    stream_mgr_init(StreamMgr*);
void    stream_mgr_free(StreamMgr*);
bool    stream_mgr_create(StreamMgr*, Tr*, StreamConfig*, bool);
bool    stream_mgr_drop(StreamMgr*, Tr*, Str*, Str*, bool);
void    stream_mgr_drop_of(StreamMgr*, Tr*, Stream*);
bool    stream_mgr_rename(StreamMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool    stream_mgr_grant(StreamMgr*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void    stream_mgr_dump(StreamMgr*, Buf*);
Buf*    stream_mgr_list(StreamMgr*, Str*, Str*, int);
Stream* stream_mgr_find(StreamMgr*, Str*, Str*, bool);
