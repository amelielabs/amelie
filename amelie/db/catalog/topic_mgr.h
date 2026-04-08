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

typedef struct TopicMgr TopicMgr;

struct TopicMgr
{
	RelMgr mgr;
};

void topic_mgr_init(TopicMgr*);
void topic_mgr_free(TopicMgr*);
bool topic_mgr_create(TopicMgr*, Tr*, TopicConfig*, bool);
bool topic_mgr_drop(TopicMgr*, Tr*, Str*, Str*, bool);
void topic_mgr_drop_of(TopicMgr*, Tr*, Topic*);
bool topic_mgr_rename(TopicMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool topic_mgr_grant(TopicMgr*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void topic_mgr_dump(TopicMgr*, Buf*);
Buf* topic_mgr_list(TopicMgr*, Str*, Str*, int);
Topic*
topic_mgr_find(TopicMgr*, Str*, Str*, bool);
