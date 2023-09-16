#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Var Var;

enum
{
	// hidden
	VAR_H = 1,
	// secret
	VAR_S = 2,
	// can be set on start in config
	VAR_C = 4,
	// can be updated in runtime
	VAR_R = 8,
	// persistent
	VAR_P = 16
};

typedef enum
{
	VAR_BOOL,
	VAR_INT,
	VAR_STRING,
	VAR_DATA
} VarType;

struct Var
{
	int     flags;
	VarType type;
	Str     name;
	List    link;
	List    link_persistent;
	union
	{
		atomic_u64 integer;
		Str        string;
	};
};

static inline void
var_init(Var* self, const char* name, VarType type, int flags)
{
	memset(self, 0, sizeof(Var));
	self->type  = type;
	self->flags = flags;
	str_set_cstr(&self->name, name);
	list_init(&self->link);
	list_init(&self->link_persistent);
}

static inline void
var_free(Var* self)
{
	if (self->type == VAR_STRING || self->type == VAR_DATA)
		str_free(&self->string);
}

static inline bool
var_is(Var* self, int flag)
{
	return (self->flags & flag) > 0;
}

// int
static inline void
var_int_set(Var* self, uint64_t value)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	atomic_u64_set(&self->integer, value);
}

static inline uint64_t
var_int_set_next(Var* self)
{
	uint64_t value;
	value = atomic_u64_inc(&self->integer);
	return value + 1;
}

static inline void
var_int_follow(Var* self, uint64_t value)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	// todo: cas loop
	if (value > atomic_u64_of(&self->integer))
		atomic_u64_set(&self->integer, value);
}

static inline uint64_t
var_int_of(Var* self)
{
	assert(self->type == VAR_INT || self->type == VAR_BOOL);
	return atomic_u64_of(&self->integer);
}

// string
static inline bool
var_string_is_set(Var* self)
{
	assert(self->type == VAR_STRING);
	return !str_empty(&self->string);
}

static inline void
var_string_set(Var* self, Str* str)
{
	assert(self->type == VAR_STRING);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline void
var_string_set_raw(Var* self, const char* value, int size)
{
	assert(self->type == VAR_STRING);
	str_free(&self->string);
	str_strndup(&self->string, value, size);
}

static inline char*
var_string_of(Var* self)
{
	assert(self->type == VAR_STRING);
	return str_of(&self->string);
}

// data
static inline bool
var_data_is_set(Var* self)
{
	assert(self->type == VAR_DATA);
	return !str_empty(&self->string);
}

static inline void
var_data_set(Var* self, uint8_t* value, int size)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_strndup(&self->string, value, size);
}

static inline void
var_data_set_buf(Var* self, Buf* buf)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_strndup(&self->string, buf->start, buf_size(buf));
}

static inline void
var_data_set_str(Var* self, Str* str)
{
	assert(self->type == VAR_DATA);
	str_free(&self->string);
	str_copy(&self->string, str);
}

static inline uint8_t*
var_data_of(Var* self)
{
	assert(self->type == VAR_DATA);
	return str_u8(&self->string);
}

static inline void
var_print(Var* self) 
{
	switch (self->type) {
	case VAR_BOOL:
		log("%-32s%s", str_of(&self->name),
		    var_int_of(self) ? "true" : "false");
		break;
	case VAR_INT:
		log("%-32s%" PRIu64, str_of(&self->name),
		    var_int_of(self));
		break;
	case VAR_STRING:
		log("%-32s%.*s", str_of(&self->name),
		    str_size(&self->string), str_of(&self->string));
		break;
	case VAR_DATA:
		break;
	}
}

static inline void
var_msg_write(Var* self, Buf* buf)
{
	switch (self->type) {
	case VAR_STRING:
		encode_string(buf, &self->string);
		break;
	case VAR_DATA:
		if (var_data_is_set(self))
			buf_write_str(buf, &self->string);
		else
			encode_null(buf);
		break;
	case VAR_BOOL:
		encode_bool(buf, var_int_of(self));
		break;
	case VAR_INT:
		encode_integer(buf, var_int_of(self));
		break;
	}
}

static inline Buf*
var_msg_create(Var* self)
{
	auto buf = msg_create(MSG_OBJECT);
	var_msg_write(self, buf);
	msg_end(buf);
	return buf;
}
