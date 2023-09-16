#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Global Global;

struct Global
{
	Config*   config;
	Control*  control;
	UuidMgr*  uuid_mgr;
	Resolver* resolver;
};

#define global() ((Global*)mn_task->main_arg_global)
#define config()  global()->config

static inline const char*
config_directory(void)
{
	return var_string_of(&config()->directory);
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

// control
static inline void
control_save_config(void)
{
	global()->control->save_config(global()->control->arg);
}

static inline void
control_checkpoint(bool full)
{
	global()->control->checkpoint(global()->control->arg, full);
}

// error injection
#define error_injection(name) \
	if (unlikely(var_int_of(config()->name))) \
		error("(%s:%d) error injection: %s", source_file, source_line, #name)
