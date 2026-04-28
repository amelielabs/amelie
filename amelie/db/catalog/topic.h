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

typedef struct Topic Topic;

struct Topic
{
	Rel          rel;
	int          cdc;
	TopicConfig* config;
};

bool topic_create(Catalog*, Tr*, TopicConfig*, bool);
bool topic_drop(Catalog*, Tr*, Str*, Str*, bool);
void topic_drop_of(Catalog*, Tr*, Topic*);
bool topic_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool topic_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);

always_inline static inline Topic*
topic_of(Rel* self)
{
	return (Topic*)self;
}
