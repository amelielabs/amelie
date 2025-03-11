
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

static bool resource_ro = false;

static void
test_lock_write_main(void* arg)
{
	Resource* r = arg;
	auto lock = resource_lock(r, true);
	resource_ro = true;
	resource_unlock(lock);
}

void
test_lock_write(void* arg)
{
	unused(arg);
	LockCache cache;
	lock_cache_init(&cache);

	Resource r;
	resource_init(&r, &cache);
	test(! resource_is_locked(&r) );

	auto lock = resource_lock(&r, false);
	test( resource_is_locked(&r) );

	uint64_t id;
	id = coroutine_create(test_lock_write_main, &r);
	test( id != 0 );

	coroutine_sleep(0);
	test(! resource_ro);
	resource_unlock(lock);

	coroutine_sleep(0);

	test(resource_ro);
	coroutine_wait(id);

	test(! resource_is_locked(&r) );
	lock_cache_free(&cache);
}

static int resource_readers = 0;

static void
test_lock_write_readers_main(void* arg)
{
	Resource* r = arg;
	auto lock = resource_lock(r, true);
	resource_readers++;
	resource_unlock(lock);
}

void
test_lock_write_readers(void* arg)
{
	unused(arg);
	LockCache cache;
	lock_cache_init(&cache);

	Resource r;
	resource_init(&r, &cache);
	test(! resource_is_locked(&r) );

	auto lock = resource_lock(&r, false);
	test( resource_is_locked(&r) );

	uint64_t id0;
	id0 = coroutine_create(test_lock_write_readers_main, &r);
	test( id0 != 0 );

	uint64_t id1;
	id1 = coroutine_create(test_lock_write_readers_main, &r);
	test( id1 != 0 );

	coroutine_sleep(0);

	test(resource_readers == 0);
	resource_unlock(lock);
	coroutine_sleep(0);
	coroutine_sleep(0);

	test(resource_readers == 2);
	test(! resource_is_locked(&r) );

	coroutine_wait(id0);
	coroutine_wait(id1);

	lock_cache_free(&cache);
}

static bool resource_rw = false;

static void
test_lock_read0_main(void* arg)
{
	Resource* r = arg;
	auto lock = resource_lock(r, false);
	resource_rw = true;
	resource_unlock(lock);
}

void
test_lock_read0(void* arg)
{
	unused(arg);
	LockCache cache;
	lock_cache_init(&cache);

	Resource r;
	resource_init(&r, &cache);
	test(! resource_is_locked(&r) );

	auto lock = resource_lock(&r, true);
	test( resource_is_locked(&r) );

	uint64_t id;
	id = coroutine_create(test_lock_read0_main, &r);
	test( id != 0 );

	coroutine_sleep(0);
	test(! resource_rw);
	resource_unlock(lock);

	coroutine_sleep(0);

	test(resource_rw);
	coroutine_wait(id);

	test(! resource_is_locked(&r) );
	lock_cache_free(&cache);
}

static void
test_lock_read1_main(void* arg)
{
	Resource* r = arg;
	auto lock = resource_lock(r, true);
	resource_ro = true;
	resource_unlock(lock);
}

void
test_lock_read1(void* arg)
{
	unused(arg);
	LockCache cache;
	lock_cache_init(&cache);

	Resource r;
	resource_init(&r, &cache);
	test(! resource_is_locked(&r) );

	resource_ro = false;

	auto lock = resource_lock(&r, true);
	test( resource_is_locked(&r) );

	uint64_t id;
	id = coroutine_create(test_lock_read1_main, &r);
	test( id != 0 );

	coroutine_sleep(0);
	test(resource_ro);
	resource_unlock(lock);

	test(! resource_is_locked(&r));
	coroutine_wait(id);

	lock_cache_free(&cache);
}
