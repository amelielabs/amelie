#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct RecoverIf RecoverIf;
typedef struct Recover   Recover;

struct RecoverIf
{
	void (*build_index)(Recover*, Table*, IndexConfig*);
	void (*build_column_add)(Recover*, Table*, Table*, Column*);
	void (*build_column_drop)(Recover*, Table*, Table*, int);
};

struct Recover
{
	Transaction trx;
	WalBatch    batch;
	RecoverIf*  iface;
	void*       iface_arg;
	Db*         db;
};

void recover_init(Recover*, Db*, RecoverIf*, void*);
void recover_free(Recover*);
void recover_next(Recover*, uint8_t**, uint8_t**);
void recover_next_write(Recover*, WalWrite*, bool, int);
void recover_wal(Recover*);
