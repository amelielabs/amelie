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

#define runtime() ((Runtime*)am_runtime)
#define config()  (&runtime()->config)
#define state()   (&runtime()->state)

// directory
static inline const char*
state_directory(void)
{
	return str_of(&state()->directory);
}

// recover
static inline void
state_recover_set(int value)
{
	atomic_u32_set(&state()->recover, value);
}

// lsn
static inline uint64_t
state_lsn(void)
{
	return atomic_u64_of(&state()->lsn);
}

static inline void
state_lsn_set(uint64_t value)
{
	atomic_u64_set(&state()->lsn, value);
}

static inline uint64_t
state_lsn_next(void)
{
	return atomic_u64_inc(&state()->lsn) + 1;
}

// replication
static inline void
state_primary_set(Uuid* id)
{
	state()->primary = *id;
}

static inline bool
state_is_primary(void)
{
	return uuid_empty(&state()->primary);
}

// secret
static inline void
state_secret_set(Str* value)
{
	str_assign(&state()->secret, value);

	// invalidation event
	state()->secret_seq++;
}

// control
static inline void
control_state_write(void)
{
	runtime()->iface->state_write(runtime()->iface->arg);
}

static inline void
control_state_read(Buf* buf)
{
	return runtime()->iface->state_read(runtime()->iface->arg, buf);
}

// lock manager
#define lock(resource, id) \
	locks_lock(&runtime()->locks, (resource), (id), NULL, source_function)

#define lock_system(resource_id, id) \
	lock(&runtime()->lockables.list[(resource_id)].res, (id))

#define lock_storage(id) lock_system(RES_STORAGE, id)

#define breakpoint(res_id) \
({ \
	auto ref = &runtime()->lockables.list[(res_id)]; \
	if (unlikely(atomic_u32_of(&ref->bp_refs) > 0)) \
		unlock(locks_lock(&runtime()->locks, &ref->res, LOCK_SHARED, \
		       NULL, \
		       source_function)); \
})

hot static inline void
unlock(Lock* self)
{
	locks_unlock(&runtime()->locks, self);
}

hot static inline void
unlock_all(void)
{
	// unlock all locks taken by the coroutine
	auto locks = &am_self()->locks;
	list_foreach_reverse_safe(locks)
		unlock(list_at(Lock, link));
	list_init(locks);
}

// execute function in background
static inline void
run(WorkerMain main, int argc, ...)
{
	// prepare arguments
	intptr_t argv[argc];
	va_list args;
	va_start(args, argc);
	int i = 0;
	for (; i < argc; i++)
		argv[i] = va_arg(args, intptr_t);
	va_end(args);

	// reset error
	auto error = &am_self()->error;
	error->code = ERROR_NONE;

	// add request and wait for completion
	WorkerReq req;
	worker_req_init(&req, error, main, argv);
	event_attach(&req.on_complete);
	workers_add(&runtime()->workers, &req);

	cancel_pause();
	event_wait(&req.on_complete, -1);
	cancel_resume();

	// rethrow on error
	if (unlikely(error->code != ERROR_NONE))
		rethrow();
}

static inline void
resolve(char* addr, int port, struct addrinfo** result)
{
	run(socket_getaddrinfo_job, 3, addr, port, result);
}
