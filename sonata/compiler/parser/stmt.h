#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Stmt     Stmt;
typedef struct StmtList StmtList;

typedef enum
{
	STMT_UNDEF,
	STMT_SHOW,
	STMT_SET,
	STMT_CHECKPOINT,
	STMT_CREATE_USER,
	STMT_CREATE_SCHEMA,
	STMT_CREATE_TABLE,
	STMT_CREATE_VIEW,
	STMT_DROP_USER,
	STMT_DROP_SCHEMA,
	STMT_DROP_TABLE,
	STMT_DROP_VIEW,
	STMT_ALTER_USER,
	STMT_ALTER_SCHEMA,
	STMT_ALTER_TABLE,
	STMT_ALTER_VIEW,
	STMT_INSERT,
	STMT_UPDATE,
	STMT_DELETE,
	STMT_SELECT
} StmtId;

struct Stmt
{
	StmtId     id;
	Ast*       ast;
	int        order;
	Ast*       name;
	Def        def;
	TargetList target_list;
	StmtList*  stmt_list;
	Lex*       lex;
	Db*        db;
	List       link;
};

struct StmtList
{
	int  list_count;
	List list;
};

static inline Stmt*
stmt_allocate(Db* db, Lex* lex, StmtList* stmt_list)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id        = STMT_UNDEF;
	self->ast       = NULL;
	self->order     = 0;
	self->name      = NULL;
	self->stmt_list = stmt_list;
	self->lex       = lex;
	self->db        = db;
	def_init(&self->def);
	target_list_init(&self->target_list);
	list_init(&self->link);
	return self;
}

always_inline static inline Ast*
stmt_next_shadow(Stmt* self)
{
	lex_set_keywords(self->lex, false);
	auto ast = lex_next(self->lex);
	lex_set_keywords(self->lex, true);
	return ast;
}

always_inline static inline Ast*
stmt_next(Stmt* self)
{
	return lex_next(self->lex);
}

always_inline static inline Ast*
stmt_if(Stmt* self, int id)
{
	return lex_if(self->lex, id);
}

always_inline static inline void
stmt_push(Stmt* self, Ast* ast)
{
	lex_push(self->lex, ast);
}

static inline bool
stmt_is_utility(Stmt* self)
{
	switch (self->id) {
	case STMT_SHOW:
	case STMT_SET:
	case STMT_CHECKPOINT:
	case STMT_CREATE_USER:
	case STMT_CREATE_SCHEMA:
	case STMT_CREATE_TABLE:
	case STMT_CREATE_VIEW:
	case STMT_DROP_USER:
	case STMT_DROP_SCHEMA:
	case STMT_DROP_TABLE:
	case STMT_DROP_VIEW:
	case STMT_ALTER_USER:
	case STMT_ALTER_SCHEMA:
	case STMT_ALTER_TABLE:
	case STMT_ALTER_VIEW:
		return true;
	default:
		break;
	}
	return false;
}

static inline void
stmt_list_init(StmtList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
stmt_list_add(StmtList* self, Stmt* stmt)
{
	stmt->order = self->list_count;
	self->list_count++;
	list_append(&self->list, &stmt->link);
}

static inline Stmt*
stmt_list_find(StmtList* self, Str* name) 
{
	list_foreach(&self->list)
	{
		auto stmt = list_at(Stmt, link);
		if (stmt->name == NULL)
			continue;
		if (str_compare(&stmt->name->string, name))
			return stmt;
	}
	return NULL;
}

static inline int
stmt_max_cte_order(Stmt* self)
{
	auto order  = -1;
	auto target = self->target_list.list;
	while (target)
	{
		if (target->cte)
		{
			if (target->cte->order > order)
				order = target->cte->order;
		}
		target = target->next;
	}
	return order;
}
