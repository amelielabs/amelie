#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void part_insert(Part*, Transaction*, bool, uint8_t**);
void part_update(Part*, Transaction*, Iterator*, uint8_t**);
void part_delete(Part*, Transaction*, Iterator*);
void part_delete_by(Part*, Transaction*, uint8_t**);
void part_upsert(Part*, Transaction*, Iterator**, uint8_t**);
void part_ingest(Part*, uint8_t**);
