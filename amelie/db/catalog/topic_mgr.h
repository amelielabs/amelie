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

bool topic_mgr_create(Catalog*, Tr*, TopicConfig*, bool);
bool topic_mgr_drop(Catalog*, Tr*, Str*, Str*, bool);
void topic_mgr_drop_of(Catalog*, Tr*, Topic*);
bool topic_mgr_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool topic_mgr_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
void topic_mgr_dump(Catalog*, Buf*);
