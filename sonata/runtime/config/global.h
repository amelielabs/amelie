#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Global Global;

struct Global
{
	Config*   config;
	Control*  control;
	Random*   random;
	Resolver* resolver;
};

#define global() ((Global*)so_task->main_arg_global)
#define config()  global()->config

// control
static inline void
control_save_config(void)
{
	global()->control->save_config(global()->control->arg);
}

// directory
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

// ssn
static inline uint64_t
config_ssn(void)
{
	return var_int_of(&config()->ssn);
}

static inline void
config_ssn_set(uint64_t value)
{
	return var_int_set(&config()->ssn, value);
}

static inline uint64_t
config_ssn_next(void)
{
	return var_int_set_next(&config()->ssn);
}

static inline void
config_ssn_follow(uint64_t value)
{
	return var_int_follow(&config()->ssn, value);
}
