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

typedef struct SltCmd SltCmd;

typedef enum
{
	SLT_UNDEF,
	SLT_QUERY,
	SLT_STATEMENT_OK,
	SLT_STATEMENT_ERROR,
	SLT_HASH_THRESHOLD,
	SLT_HALT
} SltType;

typedef enum
{
	SLT_SORT_NONE,
	SLT_SORT_ROW,
	SLT_SORT_VALUE
} SltSort;

struct SltCmd
{
	SltType type;
	Str     cmd;
	Str     query;
	Str     result;
	Str     body;
	int     line;
	union {
		struct {
			int     columns;
			SltSort sort;
		};
		int value;
	};
};

static inline void
slt_cmd_init(SltCmd* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
slt_cmd_reset(SltCmd* self)
{
	slt_cmd_init(self);
}

static inline void
slt_cmd_log(SltCmd* self)
{
	info("---- (line %d)", self->line);
	info("%.*s", (int)str_size(&self->body), str_of(&self->body));
}

static inline void
slt_cmd_error(SltCmd* self, char* fmt, ...)
{
	char msg[256];
	va_list args;
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);

	slt_cmd_log(self);
	error("%s", msg);
}

static inline void
slt_cmd_error_invalid(SltCmd* self)
{
	slt_cmd_error(self, "invalid command");
}
