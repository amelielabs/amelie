
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

#include <amelie_test.h>

void
test_msg(void* arg)
{
	unused(arg);
	auto buf = msg_create(123);
	auto msg = msg_of(buf);
	test(msg->id == 123);
	test(msg->size == sizeof(Msg));
	buf_write(buf, "hello", 5);
	msg_end(buf);

	msg = msg_of(buf);
	test(msg->size == sizeof(Msg) + 5);
	test( !memcmp(msg->data, "hello", 5) );
	buf_free(buf);
}
