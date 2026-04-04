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

typedef struct ChannelMgr ChannelMgr;

struct ChannelMgr
{
	RelMgr mgr;
};

void channel_mgr_init(ChannelMgr*);
void channel_mgr_free(ChannelMgr*);
bool channel_mgr_create(ChannelMgr*, Tr*, ChannelConfig*, bool);
bool channel_mgr_drop(ChannelMgr*, Tr*, Str*, Str*, bool);
void channel_mgr_drop_of(ChannelMgr*, Tr*, Channel*);
bool channel_mgr_rename(ChannelMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool channel_mgr_grant(ChannelMgr*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void channel_mgr_dump(ChannelMgr*, Buf*);
Buf* channel_mgr_list(ChannelMgr*, Str*, Str*, int);
Channel*
channel_mgr_find(ChannelMgr*, Str*, Str*, bool);
