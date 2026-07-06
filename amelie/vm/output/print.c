
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

void
print_init(Print* self)
{
	self->cols_count = 0;
	self->columns    = NULL;
	self->value      = NULL;
	self->size       = 0;
	self->size_max   = 120;
	self->buf        = NULL;
	self->tz         = NULL;
	buf_init(&self->cols);
	str_set(&self->chr_cut, "…", sizeof("…") - 1);
	str_set(&self->chr_line, "─", sizeof("─") - 1);
}

void
print_free(Print* self)
{
	buf_free(&self->cols);
}

void
print_reset(Print* self)
{
	self->cols_count = 0;
	self->columns    = NULL;
	self->value      = NULL;
	self->size       = 0;
	self->buf        = NULL;
	self->tz         = NULL;
	buf_reset(&self->cols);
}

hot static inline void
print_estimate(Print* self)
{
	// estimate columns values sizes
	auto columns = self->columns;

	if (columns->count == 1)
	{
		auto first = columns_first(columns);
		auto col = print_at(self, 0);
		col->column      = first;
		col->size_header = str_size(&first->name);
		col->size        = col->size_header;
		col->pretty      = true;
		self->cols_count = 1;
		self->size       = col->size_header;
		return;
	}

	auto value = self->value;
	if (value->type == TYPE_STORE)
	{
		auto it = store_iterator(value->store);
		defer(store_iterator_close, it);

		auto   count = 0;
		Value* row;
		while ((row = store_iterator_at(it)) && count < 100)
		{
			list_foreach(&columns->list)
			{
				auto column = list_at(Column, link);
				auto col  = print_at(self, column->order);
				auto size = value_print_estimate(row + column->order, self->tz, false, self->buf);
				if (size > col->size_value)
					col->size_value = size;
			}
			store_iterator_next(it);
			count++;
		}
		return;
	}

	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto col  = print_at(self, column->order);
		auto size = value_print_estimate(value, self->tz, false, self->buf);
		if (size > col->size_value)
			col->size_value = size;
	}
}

void
print_create(Print* self, Columns* columns, Value* value, Timezone* tz, Buf* buf)
{
	// allocate columns
	auto cols_size = sizeof(PrintCol) * columns->count;
	auto cols = (PrintCol*)buf_emplace(&self->cols, cols_size);
	memset(cols, 0, cols_size);

	// prepare
	self->cols_count = 0;
	self->columns    = columns;
	self->value      = value;
	self->size       = 0;
	self->buf        = buf;
	self->tz         = tz;

	// estimate columns values sizes
	print_estimate(self);

	// special case for a single column
	if (columns->count == 1)
		return;

	// arrange columns
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto col = print_at(self, column->order);
		col->column = column;

		// max fit
		col->size_header = utf8_strlen(&column->name);
		if ((self->size + col->size_header + 2) > self->size_max)
			break;

		auto left = self->size_max - self->size;
		col->size = col->size_header;
		if (col->size_value > col->size)
			col->size = col->size_value;

		// padding
		col->size += 2;

		auto is_last = list_is_last(&columns->list, &column->link);
		if (col->size >= left)
		{
			col->size = left;
		} else
		{
			if (col->size > 50)
				if (! is_last)
					col->size = 50;
		}

		self->size += col->size;
		self->cols_count++;
	}

	buf_reset(buf);
}

static inline void
print_ensure_limit(Print* self)
{
	auto limit = opt_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self->buf) >= limit))
		error("output limit reached");
}

static void
print_pad(Print* self, int n)
{
	auto buf = self->buf;
	buf_reserve(buf, n);
	memset(buf->position, ' ', n);
	buf_advance(buf, n);
}

static void
print_columns(Print* self)
{
	// print columns names
	auto buf  = self->buf;
	auto it   = print_at(self, 0);
	auto last = print_at(self, self->cols_count - 1);
	while (it <= last)
	{
		buf_write_str(buf, &it->column->name);
		if (it != last)
			print_pad(self, it->size - it->size_header);
		it++;
	}

	// truncate
	// …
	if (self->cols_count != self->columns->count)
	{
		buf_truncate(buf, 1);
		buf_write_str(buf, &self->chr_cut);
	}
	buf_write(buf, "\n", 1);

	// ──────────
	for (auto i = 0; i < self->size; i++)
		buf_write_str(buf, &self->chr_line);
	buf_write(buf, "\n", 1);
}

static void
print_row(Print* self, Value* row)
{
	auto buf  = self->buf;
	auto it   = print_at(self, 0);
	auto last = print_at(self, self->cols_count - 1);

	for (; it <= last; it++)
	{
		auto start = buf_size(buf);
		value_print(row + it->column->order, self->tz, it->pretty, buf);
		if (it->pretty)
			continue;

		// cut
		// …
		auto size = buf_size(buf) - start;
		if (size > it->size)
		{
			buf_truncate(buf, (size - it->size) + 1);
			buf_write_str(buf, &self->chr_cut);
			size = it->size;
		}

		// pad
		if (it != last)
			print_pad(self, it->size - size);
	}

	buf_write(buf, "\n", 1);
	print_ensure_limit(self);
}

void
print_run(Print* self)
{
	// columns
	auto buf = self->buf;
	buf_write(buf, "\n", 1);
	print_columns(self);

	// rows
	if (self->value->type == TYPE_STORE)
	{
		auto it = store_iterator(self->value->store);
		defer(store_iterator_close, it);
		Value* row;
		for (; (row = store_iterator_at(it)); store_iterator_next(it))
			print_row(self, row);
		return;
	}

	print_row(self, self->value);
}
