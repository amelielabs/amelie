#pragma once

//
// amelie.
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
	STMT_START_REPL,
	STMT_STOP_REPL,
	STMT_PROMOTE,
	STMT_CHECKPOINT,
	STMT_CREATE_USER,
	STMT_CREATE_REPLICA,
	STMT_CREATE_NODE,
	STMT_CREATE_SCHEMA,
	STMT_CREATE_TABLE,
	STMT_CREATE_INDEX,
	STMT_CREATE_VIEW,
	STMT_DROP_USER,
	STMT_DROP_REPLICA,
	STMT_DROP_NODE,
	STMT_DROP_SCHEMA,
	STMT_DROP_TABLE,
	STMT_DROP_INDEX,
	STMT_DROP_VIEW,
	STMT_ALTER_USER,
	STMT_ALTER_SCHEMA,
	STMT_ALTER_TABLE,
	STMT_ALTER_INDEX,
	STMT_ALTER_VIEW,
	STMT_TRUNCATE,
	STMT_INSERT,
	STMT_UPDATE,
	STMT_DELETE,
	STMT_SELECT,
	STMT_RETURN,
	STMT_EXECUTE,
	STMT_WATCH
} StmtId;

struct Stmt
{
	StmtId       id;
	Ast*         ast;
	int          order;
	bool         ret;
	Cte*         cte;
	CteList*     cte_list;
	CteDeps      cte_deps;
	Columns*     args;
	TargetList   target_list;
	StmtList*    stmt_list;
	CodeData*    data;
	Json*        json;
	Lex*         lex;
	FunctionMgr* function_mgr;
	Local*       local;
	Db*          db;
	List         link;
};

struct StmtList
{
	int  list_count;
	List list;
};

static inline Stmt*
stmt_allocate(Db*          db,
              FunctionMgr* function_mgr,
              Local*       local,
              Lex*         lex,
              CodeData*    data,
              Json*        json,
              StmtList*    stmt_list,
              CteList*     cte_list,
              Columns*     args)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id           = STMT_UNDEF;
	self->ast          = NULL;
	self->order        = 0;
	self->ret          = false;
	self->cte          = NULL;
	self->cte_list     = cte_list;
	self->args         = args;
	self->stmt_list    = stmt_list;
	self->data         = data;
	self->json         = json;
	self->lex          = lex;
	self->function_mgr = function_mgr;
	self->local        = local;
	self->db           = db;
	cte_deps_init(&self->cte_deps);
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
	case STMT_START_REPL:
	case STMT_STOP_REPL:
	case STMT_PROMOTE:
	case STMT_CHECKPOINT:
	case STMT_CREATE_USER:
	case STMT_CREATE_REPLICA:
	case STMT_CREATE_NODE:
	case STMT_CREATE_SCHEMA:
	case STMT_CREATE_TABLE:
	case STMT_CREATE_INDEX:
	case STMT_CREATE_VIEW:
	case STMT_DROP_USER:
	case STMT_DROP_REPLICA:
	case STMT_DROP_NODE:
	case STMT_DROP_SCHEMA:
	case STMT_DROP_TABLE:
	case STMT_DROP_INDEX:
	case STMT_DROP_VIEW:
	case STMT_ALTER_USER:
	case STMT_ALTER_SCHEMA:
	case STMT_ALTER_TABLE:
	case STMT_ALTER_INDEX:
	case STMT_ALTER_VIEW:
	case STMT_TRUNCATE:
	case STMT_EXECUTE:
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
