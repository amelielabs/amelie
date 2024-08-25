
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>
#include <amelie_test.h>

void
test_msg(void* arg)
{
	auto buf = msg_begin(123);
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
