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

typedef struct Global Global;

struct Global
{
	Config*      config;
	State*       state;
	Control*     control;
	Timezone*    timezone;
	TimezoneMgr* timezone_mgr;
	Random*      random;
	Logger*      logger;
	Resolver*    resolver;
};

#define global() ((Global*)am_task->main_arg_global)
#define config()  global()->config
#define state()   global()->state

// control
static inline void
control_lock(void)
{
	rpc(global()->control->system, RPC_LOCK, 0);
}

static inline void
control_unlock(void)
{
	rpc(global()->control->system, RPC_UNLOCK, 0);
}

static inline void
control_save_state(void)
{
	global()->control->save_state(global()->control->arg);
}

// directory
static inline const char*
config_directory(void)
{
	return str_of(var_string_of(&state()->directory));
}

// checkpoint
static inline uint64_t
config_checkpoint(void)
{
	return var_int_of(&state()->checkpoint);
}

// lsn
static inline uint64_t
config_lsn(void)
{
	return var_int_of(&state()->lsn);
}

static inline void
config_lsn_set(uint64_t value)
{
	return var_int_set(&state()->lsn, value);
}

static inline uint64_t
config_lsn_next(void)
{
	return var_int_set_next(&state()->lsn);
}

static inline void
config_lsn_follow(uint64_t value)
{
	return var_int_follow(&state()->lsn, value);
}

// psn
static inline uint64_t
config_psn(void)
{
	return var_int_of(&state()->psn);
}

static inline void
config_psn_set(uint64_t value)
{
	return var_int_set(&state()->psn, value);
}

static inline uint64_t
config_psn_next(void)
{
	return var_int_set_next(&state()->psn);
}

static inline void
config_psn_follow(uint64_t value)
{
	return var_int_follow(&state()->psn, value);
}
