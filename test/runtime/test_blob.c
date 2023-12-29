
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo.h>
#include <indigo_test.h>

void
test_blob(void* arg)
{
	Blob blob;
	blob_init(&blob, 1024);
	blob_free(&blob);

	blob_init(&blob, 1024);
	blob_write(&blob, "hello", 5);
	test(blob_size(&blob) == 5);
	blob_free(&blob);

	blob_init(&blob, 1024);
	blob_reserve(&blob, 25);

	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	test(blob_size(&blob) == 25);
	blob_write(&blob, "hello", 5);
	test(blob_size(&blob) == 30);
	blob_free(&blob);

	blob_init(&blob, 1024);
	blob_reserve(&blob, 25);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	blob_write(&blob, "hello", 5);
	test(blob_size(&blob) == 25);
	blob_free(&blob);

	blob_init(&blob, 1024);
	blob_reserve(&blob, 8 * 1024 * 1024);
	blob_free(&blob);

	blob_init(&blob, 0);
	blob_reserve(&blob, 32 * 1024 * 1024);
	blob_free(&blob);
}
