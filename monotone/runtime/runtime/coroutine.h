#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Coroutine Coroutine;

struct Coroutine
{
	uint64_t     id;
	Context      context;
	ContextStack stack;
	ExceptionMgr exception_mgr;
	Error        error;
	Arena        arena;
	BufPool      buf_pool;
	bool         cancel;
	int          cancel_pause;
	int          cancel_pause_recv;
	char         name[32];
	MainFunction main;
	void*        main_arg;
	void*        mgr;
	Event        on_exit;
	List         link_ready;
	List         link;
};

static inline void
coroutine_init(Coroutine* self, void* mgr)
{
	self->id                = 0;
	self->cancel            = false;
	self->cancel_pause      = 0;
	self->cancel_pause_recv = 0;
	self->name[0]           = 0;
	self->main              = NULL;
	self->main_arg          = NULL;
	self->mgr               = mgr;
	memset(&self->context, 0, sizeof(self->context));
	context_stack_init(&self->stack);
	exception_mgr_init(&self->exception_mgr);
	error_init(&self->error);
	arena_init(&self->arena, 4000);
	buf_pool_init(&self->buf_pool);
	list_init(&self->link);
	list_init(&self->link_ready);
	event_init(&self->on_exit);
}

static inline void
coroutine_free(Coroutine* self)
{
	buf_pool_reset(&self->buf_pool);
	arena_reset(&self->arena);
	context_stack_free(&self->stack);
	mn_free(self);
}

static inline void
coroutine_set_name(Coroutine* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(self->name, sizeof(self->name), fmt, args);
	va_end(args);
}

hot static inline bool
coroutine_cancelled(Coroutine* self)
{
	return self->cancel;
}

static inline void
coroutine_cancel_pause(Coroutine* self)
{
	self->cancel_pause++;
}

static inline void
coroutine_cancel_resume(Coroutine* self)
{
	// cancellation point
	self->cancel_pause--;
	assert(self->cancel_pause >= 0);
	if (self->cancel_pause == 0 &&
	    self->cancel_pause_recv > 0)
	{
		self->cancel_pause_recv = 0;
		self->cancel = true;
		error_throw(&self->error, &self->exception_mgr,
		            source_file,
		            source_function,
		            source_line, CANCEL, "cancelled");
	}
}
