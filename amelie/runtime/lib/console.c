
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>

//
// The implementation below is based on the linenoise
// (github.com/antirez/linenoise)
//

void
console_init(Console* self)
{
	self->fd_in        = STDIN_FILENO;
	self->fd_out       = STDOUT_FILENO;
	self->is_openned   = false;
	self->is_tty       = isatty(self->fd_in);
	self->cols         = 0;
	self->cursor       = 0;
	self->cursor_raw   = 0;
	self->prompt       =  NULL;
	self->prompt_len   = 0;
	self->refresh      = NULL;
	self->data         = NULL;
	self->history_at   = NULL;
	memset(&self->term, 0, sizeof(self->term));
	buf_list_init(&self->history);
}

static void
console_close(Console* self);

void
console_free(Console* self)
{
	if (self->is_openned)
		console_close(self);
	if (self->refresh)
	{
		buf_free(self->refresh);
		self->refresh = NULL;
	}
	buf_list_free(&self->history);
}

void
console_save(Console* self, const char* path)
{
	// rewrite history file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_TRUNC|O_CREAT|O_RDWR, 0600);

	Buf* prev = NULL;
	list_foreach(&self->history.list)
	{
		auto buf = list_at(Buf, link);
		if (buf_empty(buf))
			continue;

		// avoid duplicates
		if (prev && buf_size(prev) == buf_size(buf))
			if (! memcmp(buf->start, prev->start, buf_size(buf)))
				continue;

		file_write_buf(&file, buf);
		file_write(&file, "\n", 1);
		prev = buf;
	}
}

void
console_load(Console* self, const char* path)
{
	// load history file
	if (! fs_exists("%s", path))
		return;

	auto data = file_import("%s", path);
	defer_buf(data);
	Str history;
	buf_str(data, &history);
	while (! str_empty(&history))
	{
		Str cmd;
		str_split(&history, &cmd, '\n');
		if (str_empty(&cmd))
		{
			str_advance(&history, 1);
			continue;
		}
		str_advance(&history, str_size(&cmd));
		auto buf = buf_create();
		buf_write_str(buf, &cmd);
		buf_list_add(&self->history, buf);
	}
}

static int
console_open(Console* self)
{
	assert(! self->is_openned);
	self->cursor     = 0;
	self->cursor_raw = 0;

	// get the current terminal columns 
	struct winsize ws;
	int rc = ioctl(1, TIOCGWINSZ, &ws);
	if (rc == -1 || !ws.ws_col)
		return -1;
	self->cols = ws.ws_col;

	// save current terminal settings
	rc = tcgetattr(self->fd_in, &self->term);
	if (rc == -1)
		return -1;

	// prepare terminal
	struct termios term = self->term;
	term.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
	term.c_oflag &= ~(OPOST);
	term.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
	term.c_cflag |=  (CS8);
	term.c_cc[VMIN]  = 1;
	term.c_cc[VTIME] = 0;

	// apply terminal settings
	rc = tcsetattr(self->fd_in, TCSAFLUSH, &term);
	if (rc == -1)
		return -1;

	self->data = buf_create();
	buf_list_add(&self->history, self->data);
	self->history_at = self->data;
	self->is_openned = true;

	// show current prompt
	if (! str_empty(self->prompt))
		vfs_write(self->fd_out, str_of(self->prompt),
		          str_size(self->prompt));

	return 0;
}

static void
console_close(Console* self)
{
	if (! self->is_openned)
		return;
	tcsetattr(self->fd_in, TCSAFLUSH, &self->term);
	self->is_openned = false;
}

static void
console_refresh(Console* self)
{
	Str data;
	str_init(&data);
	buf_str(self->data, &data);

	// cut the line from the start according to the cursor
	int cursor = self->prompt_len + self->cursor;
	while (cursor >= self->cols && !str_empty(&data))
	{
		utf8_forward(&data.pos);
		cursor--;
	}

	// cut the line from the end to fit on the screen
	auto data_len = 0;
	auto end = data.end;
	data.end = data.pos;
	while ((self->prompt_len + data_len) < self->cols)
	{
		if (data.end == end)
			break;
		utf8_forward(&data.end);
		data_len++;
	}

	// print, erase to the right and move the cursor
	auto buf = self->refresh;
	if (! buf)
	{
		buf = buf_create();
		self->refresh = buf;
	}
	buf_reset(buf);
	buf_write(buf, "\r", 1);
	buf_write_str(buf, self->prompt);
	buf_write_str(buf, &data);
	buf_write(buf, "\x1b[0K", 4);
	buf_printf(buf, "\r\x1b[%dC", cursor);

	vfs_write(self->fd_out, buf_cstr(buf), buf_size(buf));
}

static void
console_set(Console* self, Buf* as)
{
	auto data = self->data;
	buf_reset(data);
	buf_write(data, as->start, buf_size(as));
	Str str;
	buf_str(data, &str);
	self->cursor = utf8_strlen(&str);
	self->cursor_raw = buf_size(data);
}

static bool
console_read_escape(Console* self)
{
	// read the escape sequence
	char seq[3];
	if (vfs_read(self->fd_in, seq, 2) <= 0)
		return false;

	char cmd = 0;
	if (seq[0] == '[')
	{
		if (seq[1] >= '0' && seq[1] <= '9') {
			// extended escape, read additional byte
			if (vfs_read(self->fd_in, &seq[2], 1) <= 0)
				return false;
			if (seq[2] == '~' && seq[1] == '3')
				cmd = seq[1];
		} else {
			cmd = seq[1];
		}
	} else
	if (seq[0] == 'O') {
		cmd = seq[1];
	}

	auto data = self->data;
	switch (cmd) {
	case '3': // Delete
	{
		if (self->cursor_raw == buf_size(data))
			break;
		if (buf_empty(data))
			break;
		char* start = (char*)data->start + self->cursor_raw;
		char* pos   = start;
		utf8_forward(&pos);
		int   size  = pos - start;
		memmove(start, pos, (char*)data->position - pos);
		buf_truncate(data, size);
		if (self->cursor_raw > buf_size(data))
			self->cursor_raw = buf_size(data);
		break;
	}
	case 'A': // Up
	{
		if (self->history.list_count == 1)
			break;
		Buf* prev;
		if (list_is_first(&self->history.list, &self->history_at->link))
			prev = data;
		else
			prev = container_of(self->history_at->link.prev, Buf, link);
		self->history_at = prev;
		console_set(self, prev);
		break;
	}
	case 'B': // Down
	{
		if (self->history.list_count == 1)
			break;
		Buf* next;
		if (list_is_last(&self->history.list, &self->history_at->link))
			next = container_of(self->history.list.next, Buf, link);
		else
			next = container_of(self->history_at->link.next, Buf, link);
		self->history_at = next;
		console_set(self, next);
		break;
	}
	case 'C': // Right
	{
		if (self->cursor_raw == buf_size(data))
			break;
		char* start = (char*)data->start + self->cursor_raw;
		char* pos   = start;
		utf8_forward(&pos);
		self->cursor_raw += pos - start;
		self->cursor++;
		break;
	}
	case 'D': // Left
	{
		if (! self->cursor_raw)
			break;

		int size;
		if (self->cursor_raw < buf_size(data))
		{
			char* start = (char*)data->start + self->cursor_raw;
			char* pos = start - 1;
			utf8_backward(&pos);
			size = start - pos;
		} else
		{
			char* pos = (char*)data->position - 1;
			utf8_backward(&pos);
			size = (char*)data->position - pos;
		}

		self->cursor_raw -= size;
		self->cursor--;
		break;
	}
	case 'H': // Home
		self->cursor = 0;
		self->cursor_raw = 0;
		break;
	case 'F': // End
	{
		Str str;
		buf_str(data, &str);
		self->cursor = utf8_strlen(&str);
		self->cursor_raw = buf_size(data);
		break;
	}
	}

	return true;
}

static int
console_read_unicode(Console* self, char unicode[4])
{
	int rc = vfs_read(self->fd_in, &unicode[0], 1);
	if (rc <= 0)
		return rc;
	int size = utf8_sizeof(unicode[0]);
	if (size == -1 || size == 1)
		return size;
	vfs_read(self->fd_in, &unicode[1], size - 1);
	if (rc <= 0)
		return rc;
	return size;
}

static bool
console_read(Console* self)
{
	auto data = self->data;
	for (;;)
	{
		char unicode[4];
		int size = console_read_unicode(self, unicode);
		if (size <= 0)
			break;

		switch (unicode[0]) {
		case 13: // Enter
			vfs_write(self->fd_out, "\n\r", 2);
			return true;

		case 3: // Ctrl + C
		case 4: // Ctrl + D
			return false;

		case 27: // Escape
			if (! console_read_escape(self))
				break;
			break;

		case 127: // Backspace
		{
			if (! self->cursor)
				break;
			if (self->cursor_raw < buf_size(data))
			{
				char* start = (char*)data->start + self->cursor_raw;
				char* pos = start - 1;
				utf8_backward(&pos);
				size = start - pos;
				memmove(pos, start, (char*)data->position - start);
			} else
			{
				// last char
				char* pos = (char*)data->position - 1;
				utf8_backward(&pos);
				size = (char*)data->position - pos;
			}
			buf_truncate(data, size);

			self->cursor_raw -= size;
			self->cursor--;
			break;
		}

		default: // insert or append
		{
			buf_reserve(self->data, size);
			if (self->cursor_raw < buf_size(data))
			{
				memmove(data->start + self->cursor_raw + size,
				        data->start + self->cursor_raw,
				        buf_size(data) - self->cursor_raw);
				memcpy(data->start + self->cursor_raw, unicode, size);
				buf_advance(data, size);
			} else {
				buf_append(data, unicode, size);
			}
			self->cursor_raw += size;
			self->cursor++;
			break;
		}
		}

		console_refresh(self);
	}

	return false;
}

static inline bool
console_stream(Console* self, Str* input)
{
	auto data = self->data;
	if (! data)
	{
		data = buf_create();
		self->data = data;
	}
	buf_reset(data);
	for (;;)
	{
		char chr = fgetc(stdin);
		if (chr == EOF)
		{
			if (buf_empty(data))
				return false;
			break;
		}
		if (chr == '\n')
			break;
		buf_write(data, &chr, 1);
	}
	buf_str(self->data, input);
	return true;
}

static inline bool
console_tty(Console* self, Str* prompt, Str* input)
{
	self->prompt = prompt;
	self->prompt_len = utf8_strlen(prompt);
	int rc = console_open(self);
	if (rc == -1)
		return false;
	rc = console_read(self);
	console_close(self);
	buf_str(self->data, input);
	return rc;
}

bool
console(Console* self, Str* prompt, Str* input)
{
	if (self->is_tty)
		return console_tty(self, prompt, input);
	return console_stream(self, input);
}
