
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata.h>
#include <sonata_test.h>

void
test_palloc(void* arg)
{
	test( palloc_snapshot() == 0 );

	char* a = palloc(5);
	test(a != NULL);
	memcpy(a, "hello", 5);

	char* b = palloc(5);
	test(b != NULL);
	memcpy(b, "hello", 5);

	char* c = palloc(5);
	test(c != NULL);
	memcpy(c, "hello", 5);

	test( palloc_snapshot() == 15 );

	palloc_truncate(10);

	c = palloc(5);
	test(c != NULL);
	memcpy(c, "hello", 5);

	test( palloc_snapshot() == 15 );

	palloc_truncate(0);
	test( palloc_snapshot() == 0 );

	a = palloc(5);
	test(a != NULL);
	memcpy(a, "hello", 5);

	test( palloc_snapshot() == 5 );
}
