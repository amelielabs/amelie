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
	TopicConfig* config;
};

bool topic_create(Catalog*, Tr*, TopicConfig*, bool);

always_inline static inline Topic*
topic_of(Rel* self)
{
	return (Topic*)self;
}
