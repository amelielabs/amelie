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

typedef struct TestCommand TestCommand;

struct TestCommand
{
	const char*  name;
	int          name_size;
	void       (*fn)(TestSuite*, Str*);
};

void test_command(TestSuite*, Str*);
