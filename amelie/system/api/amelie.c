
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

#include <amelie_core.h>
#include <amelie.h>

enum
{
	AMELIE_OBJ         = 0x0323410L,
	AMELIE_OBJ_SESSION = 0x04A3455L,
	AMELIE_OBJ_FREE    = 0x0f0f0f0L
};

struct amelie
{
	int      type;
	System*  system;
	Instance instance;
	Task     task;
};

struct amelie_session
{
	int       type;
	Session*  session;
	Content   content;
	Buf       content_buf;
	Str       url;
	Str       content_type;
	Task      native;
	amelie_t* amelie;
};

AMELIE_API amelie_t*
amelie_init(void)
{
	auto self = (amelie_t*)am_malloc(sizeof(amelie_t));
	self->type   = AMELIE_OBJ;
	self->system = NULL;
	instance_init(&self->instance);
	task_init(&self->task);
	return self;
}

static void
amelie_free_session_main(void* arg)
{
	amelie_session_t* self = arg;
	session_free(self->session);
	buf_free(&self->content_buf);
	task_free(&self->native);
}

AMELIE_API void
amelie_free(void* ptr)
{
	switch (*(int*)ptr) {
	case AMELIE_OBJ:
	{
		amelie_t* self = ptr;
		auto buf = msg_create_as(&self->instance.buf_mgr, RPC_STOP, 0);
		channel_write(&self->task.channel, buf);
		task_wait(&self->task);
		task_free(&self->task);
		break;
	}
	case AMELIE_OBJ_SESSION:
	{
		amelie_session_t* self = ptr;
		task_execute(&self->native, amelie_free_session_main, self);
		break;
	}
	case AMELIE_OBJ_FREE:
	{
		fprintf(stderr, "\n%s(%p): attempt to use freed object\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	default:
	{
		fprintf(stderr, "\n%s(%p): unrecognized object type\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	}
	*(int*)ptr = AMELIE_OBJ_FREE;
	am_free(ptr);
}

typedef struct
{
	amelie_t* self;
	char*     path;
	int       argc;
	char**    argv;
} AmelieArgs;

static void
amelie_main(void* arg)
{
	AmelieArgs* args = arg;
	auto self = args->self;
	auto on_error = error_catch
	(
		// start instance
		instance_start(&self->instance);
		auto bootstrap = instance_open(&self->instance, args->path,
		                               args->argc,
		                               args->argv);
		// create system object
		self->system = system_create();
		system_start(self->system, bootstrap);

		// notify cli_start about start completion
		cond_signal(&self->task.status, 0);

		// handle system requests
		system_main(self->system);
	);

	auto system = self->system;
	if (system)
	{
		system_stop(system);
		system_free(system);
		self->system = NULL;
	}
	instance_stop(&self->instance);
	instance_free(&self->instance);

	// complete
	int rc = 0;
	if (on_error)
		rc = -1;
	cond_signal(&self->task.status, rc);
}

AMELIE_API int
amelie_open(amelie_t* self, const char* path, int argc, char** argv)
{
	// start system task
	AmelieArgs args =
	{
		.self = self,
		.path = (char*)path,
		.argc = argc,
		.argv = argv
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", amelie_main, &args,
	                         &self->instance.global, NULL,
	                         logger_write, &self->instance.logger,
	                         &self->instance.buf_mgr);
	if (unlikely(rc == -1))
		return -1;

	// wait for the start completion
	rc = cond_wait(&self->task.status);
	return rc;
}

static void
amelie_connect_main(void* arg)
{
	amelie_session_t* self = arg;
	self->session = session_create(NULL);
}

AMELIE_API amelie_session_t*
amelie_connect(amelie_t* self)
{
	auto session = (amelie_session_t*)am_malloc(sizeof(amelie_session_t));
	session->type    = AMELIE_OBJ_SESSION;
	session->session = NULL;
	session->amelie  = self;
	buf_init(&session->content_buf);
	content_init(&session->content);
	content_set(&session->content, &session->content_buf);
	str_set_cstr(&session->url, "/");
	str_set_cstr(&session->content_type, "text/plain");
	task_init(&session->native);

	// create session native task
	int rc;
	rc = task_create_nothrow(&session->native, "api", NULL, NULL,
	                         &self->instance.global, NULL,
	                         logger_write, &self->instance.logger,
	                         &self->instance.buf_mgr);
	if (unlikely(rc == -1))
	{
		task_free(&self->task);
		am_free(session);
		return NULL;
	}

	// create session using native task context
	task_execute(&session->native, amelie_connect_main, session);
	assert(session->session);
	return session;
}

typedef struct
{
	amelie_session_t* self;
	char*             command;
	int               argc;
	amelie_arg_t*     argv;
	amelie_arg_t*     result;
	int               rc;
} AmelieExecuteArgs;

hot static void
amelie_execute_main(void* arg)
{
	AmelieExecuteArgs* args = arg;
	auto self = args->self;

	content_reset(&self->content);
	Str text;
	str_set_cstr(&text, args->command);
	auto on_error = session_execute(self->session, &self->url, &text,
	                                &self->content_type,
	                                &self->content);
	if (on_error)
		args->rc = -1;

	auto result = args->result;
	result->data = buf_cstr(&self->content_buf);
	result->data_size = buf_size(&self->content_buf);
}

hot AMELIE_API int
amelie_execute(amelie_session_t* self,
               const char*       command,
               int               argc,
               amelie_arg_t*     argv,
               amelie_arg_t*     result)
{
	AmelieExecuteArgs args =
	{
		.self    = self,
		.command = (char*)command,
		.argc    = argc,
		.argv    = argv,
		.result  = result,
		.rc      = 0
	};
	task_execute(&self->native, amelie_execute_main, &args);
	return args.rc;
}
