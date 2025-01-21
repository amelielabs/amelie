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

// compiler
#include "runtime/c.h"
#include "runtime/macro.h"
#include "runtime/overflow.h"

// os
#include "runtime/atomic.h"
#include "runtime/spinlock.h"
#include "runtime/mutex.h"
#include "runtime/cond_var.h"
#include "runtime/cond.h"
#include "runtime/thread.h"

// intrusive list
#include "runtime/list.h"

// exception
#include "runtime/exception.h"
#include "runtime/log.h"
#include "runtime/error.h"
#include "runtime/report.h"

// memory
#include "runtime/allocator.h"
#include "runtime/arena.h"

// string
#include "runtime/str.h"

// buffer manager
#include "runtime/buf.h"
#include "runtime/buf_cache.h"
#include "runtime/buf_list.h"
#include "runtime/buf_mgr.h"

// event loop
#include "runtime/timer_mgr.h"
#include "runtime/fd.h"
#include "runtime/poller.h"
#include "runtime/notify.h"

// cooperative multitasking
#include "runtime/event.h"
#include "runtime/context_stack.h"
#include "runtime/context.h"
#include "runtime/coroutine.h"
#include "runtime/coroutine_mgr.h"

// ipc
#include "runtime/wait.h"
#include "runtime/bus.h"
#include "runtime/msg_id.h"
#include "runtime/msg.h"
#include "runtime/channel.h"
#include "runtime/task.h"

// runtime
#include "runtime/throw.h"
#include "runtime/defer.h"
#include "runtime/runtime.h"
#include "runtime/runtime_defer.h"

// rpc
#include "runtime/rpc.h"
#include "runtime/rpc_queue.h"
