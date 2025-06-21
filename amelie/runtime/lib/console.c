
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

//
// The implementation below is based on the linenoise
// (github.com/antirez/linenoise)
//

void
console_init(Console* self)
{
	self->is_openned = false;
	self->is_tty     = false;
	self->fd_in      = STDIN_FILENO;
	self->fd_out     = STDOUT_FILENO;
	self->prompt     = NULL;
	self->cols       = 0;
	self->refresh    = NULL;
	self->buf        = NULL;
	self->buf_pos    = 0;
	memset(&self->term, 0, sizeof(self->term));
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
	if (self->buf)
	{
		buf_free(self->buf);
		self->buf = NULL;
	}
}

void
console_prepare(Console* self, Str* path)
{
	self->refresh = buf_create();
	self->is_tty = isatty(self->fd_in);
	// todo: load history
	(void)path;
}

static int
console_open(Console* self)
{
	assert(! self->is_openned);
	if (self->buf)
	{
		buf_free(self->buf);
		self->buf = NULL;
	}
	self->buf_pos = 0;

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

	self->buf = buf_create();
	self->is_openned = true;

	// show current prompt
	if (! str_empty(self->prompt))
		vfs_write(self->fd_out, str_of(self->prompt), str_size(self->prompt));

	return 0;
}

static void
console_close(Console* self)
{
	if (! self->is_openned)
		return;
	tcsetattr(self->fd_in, TCSAFLUSH, &self->term);
	// todo: place to history
	self->is_openned = false;
}

static void
console_refresh(Console* self)
{
	Str data;
	str_init(&data);
	buf_str(self->buf, &data);

	int cursor = str_size(self->prompt) + self->buf_pos;
	while (cursor >= self->cols)
	{
		str_advance(&data, 1);
		cursor--;
	}
	while (str_size(self->prompt) + str_size(&data) > self->cols)
		data.end--;

	// print, erase to the right and move the cursor
	auto buf = self->refresh;
	buf_reset(buf);
	buf_write(buf, "\r", 1);
	buf_write_str(buf, self->prompt);
	buf_write_str(buf, &data);
	buf_write(buf, "\x1b[0K", 4);
	buf_printf(buf, "\r\x1b[%dC", cursor);

	vfs_write(self->fd_out, buf_cstr(buf), buf_size(buf));
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

	auto buf = self->buf;
	switch (cmd) {
	case '3': // Delete
		if (self->buf_pos == buf_size(buf))
			break;
		if (! buf_size(buf))
			break;
		memmove(buf->start + self->buf_pos,
				buf->start + self->buf_pos + 1,
				buf_size(buf) - self->buf_pos);
		buf_truncate(buf, 1);
		if (self->buf_pos > buf_size(buf))
			self->buf_pos = buf_size(buf);
		break;
	case 'A': // Up
		// todo: history up
		break;
	case 'B': // Down
		// todo: history down
		break;
	case 'C': // Right
		if (self->buf_pos < buf_size(buf))
			self->buf_pos++;
		break;
	case 'D': // Left
		if (self->buf_pos > 0)
			self->buf_pos--;
		break;
	case 'H': // Home
		self->buf_pos = 0;
		break;
	case 'F': // End
		self->buf_pos = buf_size(buf);
		break;
	}

	return true;
}

static bool
console_read(Console* self)
{
	auto buf = self->buf;
	for (;;)
	{
		char chr;
		int rc = vfs_read(self->fd_in, &chr, 1);
		if (rc <= 0)
			break;

		switch (chr) {
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
			if (! self->buf_pos)
				break;
			memmove(buf->start + self->buf_pos,
			        buf->start + self->buf_pos + 1,
			        buf_size(buf) - self->buf_pos);
			buf_truncate(buf, 1);
			self->buf_pos--;
			break;
		}

		default:
		{
			if (! isprint(chr))
				continue;

			// insert or append
			buf_reserve(self->buf, 1);
			if (self->buf_pos < buf_size(buf))
			{
				memmove(buf->start + self->buf_pos + 1,
				        buf->start + self->buf_pos,
				        buf_size(buf) - self->buf_pos);
				buf->start[self->buf_pos] = chr;
				buf_advance(buf, 1);
			} else {
				buf_append(buf, &chr, 1);
			}
			self->buf_pos++;
			break;
		}
		}

		console_refresh(self);
	}

	return false;
}

void
console_sync(Console* self, Str* path)
{
	(void)self;
	(void)path;
	// todo: save history file
}

bool
console(Console* self, Str* prompt, Str* input)
{
	(void)input;
	if (self->is_tty)
	{
		self->prompt = prompt;
		int rc = console_open(self);
		if (rc == -1)
			return false;
		rc = console_read(self);
		console_close(self);
		buf_str(self->buf, input);
		return rc;
	}

	// todo: read from the stream directly
	return false;
}
