
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
test_buf(void* arg)
{
	unused(arg);
	Buf* buf = buf_create();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );
	buf_free(buf);
}

void
test_buf_reserve(void* arg)
{
	unused(arg);
	Buf* buf = buf_create();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );
	buf_reserve(buf, 100);
	test( !memcmp(buf->start, "hello", 5) );
	buf_free(buf);
}

void
test_buf_write(void* arg)
{
	unused(arg);
	Buf* buf = buf_create();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );

	char data[100];
	memset(data, 'x', sizeof(data));

	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));

	test( buf_size(buf) == 305 );

	test( !memcmp(buf->start, "hello", 5) );
	test( !memcmp(buf->start + 5, data, sizeof(data)) );

	buf_free(buf);
}

void
test_buf_overwrite(void* arg)
{
	unused(arg);
	Buf* buf = buf_create();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );

	char data[100];
	memset(data, 'x', sizeof(data));

	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));

	test( buf_size(buf) == 305 );
	test( !memcmp(buf->start, "hello", 5) );
	test( !memcmp(buf->start + 5, data, sizeof(data)) );

	buf_reset(buf);

	buf_append(buf, "hello", 5);
	buf_append(buf, data, sizeof(data));
	buf_append(buf, data, sizeof(data));
	buf_append(buf, data, sizeof(data));

	test( buf_size(buf) == 305 );
	test( !memcmp(buf->start, "hello", 5) );
	test( !memcmp(buf->start + 5, data, sizeof(data)) );

	buf_reset(buf);

	buf_write(buf, "hello", 5);
	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));
	buf_write(buf, data, sizeof(data));

	test( buf_size(buf) == 305 );
	test( !memcmp(buf->start, "hello", 5) );
	test( !memcmp(buf->start + 5, data, sizeof(data)) );

	buf_free(buf);
}
