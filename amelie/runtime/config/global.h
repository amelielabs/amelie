#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Global Global;

struct Global
{
	Config*      config;
	Control*     control;
	Timezone*    timezone;
	TimezoneMgr* timezone_mgr;
	Random*      random;
	Logger*      logger;
	Resolver*    resolver;
};

#define global() ((Global*)am_task->main_arg_global)
#define config()  global()->config

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
control_save_config(void)
{
	global()->control->save_config(global()->control->arg);
}

// directory
static inline const char*
config_directory(void)
{
	return str_of(var_string_of(&config()->directory));
}

// checkpoint
static inline uint64_t
config_checkpoint(void)
{
	return var_int_of(&config()->checkpoint);
}

// lsn
static inline uint64_t
config_lsn(void)
{
	return var_int_of(&config()->lsn);
}

static inline void
config_lsn_set(uint64_t value)
{
	return var_int_set(&config()->lsn, value);
}

static inline uint64_t
config_lsn_next(void)
{
	return var_int_set_next(&config()->lsn);
}

static inline void
config_lsn_follow(uint64_t value)
{
	return var_int_follow(&config()->lsn, value);
}

// psn
static inline uint64_t
config_psn(void)
{
	return var_int_of(&config()->psn);
}

static inline void
config_psn_set(uint64_t value)
{
	return var_int_set(&config()->psn, value);
}

static inline uint64_t
config_psn_next(void)
{
	return var_int_set_next(&config()->psn);
}

static inline void
config_psn_follow(uint64_t value)
{
	return var_int_follow(&config()->psn, value);
}
