#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct SessionMgr SessionMgr;

struct SessionMgr
{
	Mutex   lock;
	CondVar cond_var;
	List    list;
	int     list_count;
};

static inline void
session_mgr_init(SessionMgr* self)
{
	self->list_count = 0;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
}

static inline void
session_mgr_free(SessionMgr* self)
{
	assert(! self->list_count);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
session_mgr_add(SessionMgr* self, Session* session)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &session->link);
	self->list_count++;
	mutex_unlock(&self->lock);
}

static inline void
session_mgr_remove(SessionMgr* self, Session* session)
{
	mutex_lock(&self->lock);
	list_unlink(&session->link);
	self->list_count--;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
session_mgr_stop(SessionMgr* self)
{
	mutex_lock(&self->lock);
	list_foreach(&self->list)
	{
		auto session = list_at(Session, link);
		task_cancel(&session->task);
	}
	while (self->list_count > 0)
		cond_var_wait(&self->cond_var, &self->lock);
	mutex_unlock(&self->lock);
}
