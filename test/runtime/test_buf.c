
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie.h>
#include <amelie_test.h>

void
test_buf(void* arg)
{
	Buf* buf = buf_begin();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );
	buf_end(buf);
	buf_free(buf);
}

void
test_buf_reserve(void* arg)
{
	Buf* buf = buf_begin();
	buf_reserve(buf, 5);
	buf_append(buf, "hello", 5);
	test( !memcmp(buf->start, "hello", 5) );
	buf_reserve(buf, 100);
	test( !memcmp(buf->start, "hello", 5) );
	buf_end(buf);
	buf_free(buf);
}

void
test_buf_write(void* arg)
{
	Buf* buf = buf_begin();
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

	buf_end(buf);
	buf_free(buf);
}

void
test_buf_overwrite(void* arg)
{
	Buf* buf = buf_begin();
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

	buf_end(buf);
	buf_free(buf);
}
