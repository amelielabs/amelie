#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Recover Recover;

typedef void (*RecoverIndexate)(Recover*, Table*, IndexConfig*);

struct Recover
{
	Transaction     trx;
	WalBatch        batch;
	RecoverIndexate indexate;
	void*           indexate_arg;
	Db*             db;
};

void recover_init(Recover*, Db*, RecoverIndexate, void*);
void recover_free(Recover*);
void recover_next(Recover*, uint8_t**, uint8_t**);
void recover_next_write(Recover*, WalWrite*, bool, int);
void recover_wal(Recover*);
