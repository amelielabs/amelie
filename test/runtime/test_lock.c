
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo.h>
#include <indigo_test.h>

static bool lock_ro = false;

static void
test_lock_write_main(void *arg)
{
	Lock* lock = arg;
	auto locker = lock_lock(lock, true);
	lock_ro = true;
	lock_unlock(locker);
}

void
test_lock_write(void *arg)
{
	LockerCache cache;
	locker_cache_init(&cache);

	Lock lock;
	lock_init(&lock, &cache);
	test(! lock_is_locked(&lock) );

	auto locker = lock_lock(&lock, false);
	test( lock_is_locked(&lock) );

	uint64_t id;
	id = coroutine_create(test_lock_write_main, &lock);
	test( id != 0 );

	coroutine_sleep(0);
	test(! lock_ro);
	lock_unlock(locker);

	coroutine_sleep(0);

	test(lock_ro);
	coroutine_wait(id);

	test(! lock_is_locked(&lock) );
	locker_cache_free(&cache);
}

static int lock_readers = 0;

static void
test_lock_write_readers_main(void* arg)
{
	Lock* lock = arg;
	auto locker = lock_lock(lock, true);
	lock_readers++;
	lock_unlock(locker);
}

void
test_lock_write_readers(void *arg)
{
	LockerCache cache;
	locker_cache_init(&cache);

	Lock lock;
	lock_init(&lock, &cache);
	test(! lock_is_locked(&lock) );

	auto locker = lock_lock(&lock, false);
	test( lock_is_locked(&lock) );

	uint64_t id0;
	id0 = coroutine_create(test_lock_write_readers_main, &lock);
	test( id0 != 0 );

	uint64_t id1;
	id1 = coroutine_create(test_lock_write_readers_main, &lock);
	test( id1 != 0 );

	coroutine_sleep(0);

	test(lock_readers == 0);
	lock_unlock(locker);
	coroutine_sleep(0);
	coroutine_sleep(0);

	test(lock_readers == 2);
	test(! lock_is_locked(&lock) );

	coroutine_wait(id0);
	coroutine_wait(id1);

	locker_cache_free(&cache);
}

static bool lock_rw = false;

static void
test_lock_read0_main(void *arg)
{
	Lock* lock = arg;
	auto locker = lock_lock(lock, false);
	lock_rw = true;
	lock_unlock(locker);
}

void
test_lock_read0(void *arg)
{
	LockerCache cache;
	locker_cache_init(&cache);

	Lock lock;
	lock_init(&lock, &cache);
	test(! lock_is_locked(&lock) );

	auto locker = lock_lock(&lock, true);
	test( lock_is_locked(&lock) );

	uint64_t id;
	id = coroutine_create(test_lock_read0_main, &lock);
	test( id != 0 );

	coroutine_sleep(0);
	test(! lock_rw);
	lock_unlock(locker);

	coroutine_sleep(0);

	test(lock_rw);
	coroutine_wait(id);

	test(! lock_is_locked(&lock) );

	locker_cache_free(&cache);
}

static void
test_lock_read1_main(void *arg)
{
	Lock* lock = arg;
	auto locker = lock_lock(lock, true);
	lock_ro = true;
	lock_unlock(locker);
}

void
test_lock_read1(void *arg)
{
	LockerCache cache;
	locker_cache_init(&cache);

	lock_ro = false;

	Lock lock;
	lock_init(&lock, &cache);

	auto locker = lock_lock(&lock, true);
	test( lock_is_locked(&lock) );

	uint64_t id;
	id = coroutine_create(test_lock_read1_main, &lock);
	test( id != 0 );

	coroutine_sleep(0);
	test(lock_ro);
	lock_unlock(locker);

	test(! lock_is_locked(&lock));
	coroutine_wait(id);

	locker_cache_free(&cache);
}
