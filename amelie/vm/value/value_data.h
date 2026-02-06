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

hot static inline int
value_data_size(Value* self, Column* column, Value* refs)
{
	// identity column
	if (column->constraints.as_identity)
		return column->type_size;

	// use reference
	if (self->type == TYPE_REF)
		self = &refs[self->integer];

	// null
	if (self->type == TYPE_NULL)
		return 0;

	// fixed types
	if (column->type_size > 0)
		return column->type_size;

	// variable types
	auto size = 0;
	switch (column->type) {
	case TYPE_STRING:
		size = json_size_string(str_size(&self->string));
		break;
	case TYPE_JSON:
		size = self->json_size;
		break;
	case TYPE_VECTOR:
		size = vector_size(self->vector);
		break;
	default:
		abort();
		break;
	}
	return size;
}

hot static inline bool
value_data_write(Value*    self, Column* column,
                 Value*    refs,
                 Value*    identity,
                 uint8_t** pos)
{
	// use reference
	if (self->type == TYPE_REF)
		self = &refs[self->integer];

	if (column->constraints.as_identity)
		self = identity;

	// null
	if (self->type == TYPE_NULL)
	{
		// NOT NULL constraint
		if (unlikely(column->constraints.not_null))
			error("column '%.*s' cannot be NULL", str_size(&column->name),
			      str_of(&column->name));
		return false;
	}

	switch (column->type) {
	case TYPE_BOOL:
	case TYPE_INT:
	case TYPE_TIMESTAMP:
	case TYPE_DATE:
	{
		switch (column->type_size) {
		case 1:
			*(int8_t*)*pos = self->integer;
			*pos += sizeof(int8_t);
			break;
		case 2:
			*(int16_t*)*pos = self->integer;
			*pos += sizeof(int16_t);
			break;
		case 4:
			*(int32_t*)*pos = self->integer;
			*pos += sizeof(int32_t);
			break;
		case 8:
			*(int64_t*)*pos = self->integer;
			*pos += sizeof(int64_t);
			break;
		default:
			abort();
			break;
		}
		break;
	}
	case TYPE_DOUBLE:
	{
		switch (column->type_size) {
		case 4:
			*(float*)*pos = self->dbl;
			*pos += sizeof(float);
			break;
		case 8:
			*(double*)*pos = self->dbl;
			*pos += sizeof(double);
			break;
		default:
			abort();
			break;
		}
		break;
	}
	case TYPE_INTERVAL:
		*(Interval*)*pos = self->interval;
		*pos += sizeof(Interval);
		break;
	case TYPE_STRING:
		json_write_string(pos, &self->string);
		break;
	case TYPE_JSON:
		memcpy(*pos, self->json, self->json_size);
		*pos += self->json_size;
		break;
	case TYPE_VECTOR:
		memcpy(*pos, self->vector, vector_size(self->vector));
		*pos += vector_size(self->vector);
		break;
	case TYPE_UUID:
		*(Uuid*)*pos = self->uuid;
		*pos += sizeof(Uuid);
		break;
	}
	return true;
}
