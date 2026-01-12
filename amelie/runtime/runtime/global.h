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

// control
static inline void
control_save_state(void)
{
	runtime()->iface->save_state(runtime()->iface->arg);
}

// directory
static inline const char*
state_directory(void)
{
	return str_of(opt_string_of(&state()->directory));
}

// checkpoint
static inline uint64_t
state_checkpoint(void)
{
	return opt_int_of(&state()->checkpoint);
}

// lsn
static inline uint64_t
state_lsn(void)
{
	return opt_int_of(&state()->lsn);
}

static inline void
state_lsn_set(uint64_t value)
{
	return opt_int_set(&state()->lsn, value);
}

static inline uint64_t
state_lsn_next(void)
{
	return opt_int_set_next(&state()->lsn);
}

static inline void
state_lsn_follow(uint64_t value)
{
	return opt_int_follow(&state()->lsn, value);
}

// psn
static inline uint64_t
state_psn(void)
{
	return opt_int_of(&state()->psn);
}

static inline void
state_psn_set(uint64_t value)
{
	return opt_int_set(&state()->psn, value);
}

static inline uint64_t
state_psn_next(void)
{
	return opt_int_set_next(&state()->psn);
}

static inline void
state_psn_follow(uint64_t value)
{
	return opt_int_follow(&state()->psn, value);
}

// jobs
static inline void
run(JobFunction main, int argc, ...)
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

	// add job and wait for completion
	Job job;
	job_init(&job, error, main, argv);
	event_attach(&job.on_complete);
	job_mgr_add(&runtime()->job_mgr, &job);

	cancel_pause();
	event_wait(&job.on_complete, -1);
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
