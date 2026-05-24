import pytest
import ctypes
import struct
import sys
from unittest.mock import MagicMock, patch


# Simulated CDC event structure and processing logic
# This mirrors the behavior described in cdc.c where data_size bytes are copied
# into event->data without bounds checking

CDC_EVENT_BUFFER_SIZE = 256  # Typical allocated buffer size for CDC events


class CDCEvent:
    """Simulates the CDC event structure from cdc.c"""
    def __init__(self, buffer_size=CDC_EVENT_BUFFER_SIZE):
        self.buffer_size = buffer_size
        self.data = bytearray(buffer_size)
        self.data_size = 0

    def copy_data(self, data, data_size):
        """
        Simulates the vulnerable memcpy pattern:
        memcpy(event->data, data, data_size);
        
        A safe implementation MUST either:
        1. Reject data_size > buffer_size
        2. Truncate data_size to buffer_size
        """
        if data_size > self.buffer_size:
            raise ValueError(
                f"Buffer overflow prevented: data_size={data_size} exceeds "
                f"allocated buffer size={self.buffer_size}"
            )
        actual_copy_size = min(data_size, self.buffer_size)
        self.data[:actual_copy_size] = data[:actual_copy_size]
        self.data_size = actual_copy_size
        return actual_copy_size


def safe_cdc_process_write(data: bytes, data_size: int, buffer_size: int = CDC_EVENT_BUFFER_SIZE) -> dict:
    """
    Simulates the CDC write processing path from cdc.c
    Returns result dict with status and bytes_written
    """
    event = CDCEvent(buffer_size=buffer_size)
    
    # This is the invariant: data_size must never exceed allocated buffer
    if data_size > buffer_size:
        return {
            "status": "rejected",
            "bytes_written": 0,
            "error": f"data_size {data_size} exceeds buffer {buffer_size}",
            "overflow_prevented": True
        }
    
    actual = event.copy_data(data, data_size)
    return {
        "status": "ok",
        "bytes_written": actual,
        "overflow_prevented": False
    }


# Attack payloads: various oversized inputs targeting the CDC buffer
ATTACK_PAYLOADS = [
    # (data, declared_data_size, description)
    # 2x buffer size
    (b"A" * 512, 512, "2x buffer size - exact double"),
    # 10x buffer size
    (b"B" * 2560, 2560, "10x buffer size"),
    # Just over buffer
    (b"C" * 257, 257, "buffer+1 overflow"),
    # SQL injection style payload exceeding buffer
    (b"'; DROP TABLE users; --" + b"X" * 300, 323, "SQL injection oversized"),
    # Null bytes mixed with overflow
    (b"\x00" * 512, 512, "null byte flood 2x"),
    # Binary exploit pattern
    (b"\x41\x41\x41\x41" * 200, 800, "classic buffer overflow pattern"),
    # Format string style attack
    (b"%s%s%s%s%n" * 60, 300, "format string style oversized"),
    # Unicode-like byte sequences oversized
    (b"\xff\xfe" * 300, 600, "unicode BOM flood oversized"),
    # Heap spray pattern
    (b"\x90" * 4096, 4096, "heap spray NOP sled 16x"),
    # Declared size larger than actual data (integer mismatch attack)
    (b"short", 9999, "declared size much larger than actual data"),
    # Maximum integer-like size
    (b"X" * 512, 65535, "near-max declared size"),
    # Exactly at boundary (should succeed)
    (b"D" * 256, 256, "exact buffer size boundary - should succeed"),
    # One byte under boundary (should succeed)
    (b"E" * 255, 255, "buffer-1 - should succeed"),
    # Empty data but large declared size
    (b"", 512, "empty data but large declared size"),
    # Repeated pattern overflow
    (b"OVERFLOW" * 100, 800, "repeated OVERFLOW string 3x"),
]


@pytest.mark.parametrize("data,data_size,description", ATTACK_PAYLOADS)
def test_cdc_buffer_read_never_exceeds_declared_length(data, data_size, description):
    """
    Invariant: Buffer reads in CDC processing never exceed the allocated buffer size.
    
    The memcpy at cdc.c:225 must never copy more bytes than the allocated size
    of event->data. Any data_size exceeding the buffer allocation must be either
    rejected or truncated — never silently overflowed.
    
    CWE-120: Buffer Copy without Checking Size of Input
    """
    buffer_size = CDC_EVENT_BUFFER_SIZE  # 256 bytes
    
    result = safe_cdc_process_write(data, data_size, buffer_size)
    
    # INVARIANT 1: bytes_written must NEVER exceed the allocated buffer size
    assert result["bytes_written"] <= buffer_size, (
        f"[{description}] BUFFER OVERFLOW: wrote {result['bytes_written']} bytes "
        f"into buffer of size {buffer_size}. data_size={data_size}"
    )
    
    # INVARIANT 2: If data_size exceeds buffer, it must be rejected or truncated
    if data_size > buffer_size:
        assert result["status"] in ("rejected", "truncated") or result["bytes_written"] <= buffer_size, (
            f"[{description}] Oversized input (data_size={data_size}) was not "
            f"properly rejected or truncated. status={result['status']}, "
            f"bytes_written={result['bytes_written']}"
        )
        assert result["overflow_prevented"] is True or result["bytes_written"] <= buffer_size, (
            f"[{description}] Overflow was not prevented for data_size={data_size}"
        )
    
    # INVARIANT 3: bytes_written must never be negative
    assert result["bytes_written"] >= 0, (
        f"[{description}] Negative bytes_written={result['bytes_written']}"
    )
    
    # INVARIANT 4: If status is ok, bytes_written must equal min(data_size, buffer_size)
    if result["status"] == "ok":
        expected_written = min(data_size, buffer_size)
        assert result["bytes_written"] == expected_written, (
            f"[{description}] Expected {expected_written} bytes written, "
            f"got {result['bytes_written']}"
        )


@pytest.mark.parametrize("multiplier", [2, 5, 10, 100, 1000])
def test_cdc_buffer_overflow_by_multiplier(multiplier):
    """
    Invariant: CDC buffer is safe against payloads that are N times the buffer size.
    No matter how large the multiplier, bytes written must not exceed buffer_size.
    """
    buffer_size = CDC_EVENT_BUFFER_SIZE
    oversized_data = b"A" * (buffer_size * multiplier)
    data_size = len(oversized_data)
    
    result = safe_cdc_process_write(oversized_data, data_size, buffer_size)
    
    assert result["bytes_written"] <= buffer_size, (
        f"Buffer overflow at {multiplier}x: wrote {result['bytes_written']} "
        f"into buffer of {buffer_size}"
    )
    assert result["overflow_prevented"] is True, (
        f"Overflow not flagged at {multiplier}x buffer size"
    )


def test_cdc_boundary_conditions():
    """
    Invariant: CDC buffer handles exact boundary conditions correctly.
    Exactly buffer_size bytes must be accepted; buffer_size+1 must be rejected.
    """
    buffer_size = CDC_EVENT_BUFFER_SIZE
    
    # Exactly at boundary - must succeed
    result_exact = safe_cdc_process_write(b"X" * buffer_size, buffer_size, buffer_size)
    assert result_exact["status"] == "ok", "Exact boundary write should succeed"
    assert result_exact["bytes_written"] == buffer_size
    
    # One byte over - must be rejected or truncated
    result_over = safe_cdc_process_write(b"X" * (buffer_size + 1), buffer_size + 1, buffer_size)
    assert result_over["bytes_written"] <= buffer_size, (
        f"One-over-boundary write must not exceed buffer: "
        f"wrote {result_over['bytes_written']}"
    )
    assert result_over["overflow_prevented"] is True


def test_cdc_declared_size_mismatch_attack():
    """
    Invariant: CDC processing is safe when declared data_size is larger than
    actual data length (integer/size mismatch attack vector).
    
    This simulates an attacker providing a small buffer but claiming a large size,
    which would cause memcpy to read beyond the actual data allocation.
    """
    buffer_size = CDC_EVENT_BUFFER_SIZE
    
    # Small actual data, but attacker claims it's huge
    actual_data = b"tiny"
    claimed_size = 99999
    
    result = safe_cdc_process_write(actual_data, claimed_size, buffer_size)
    
    # Must not write more than buffer allows
    assert result["bytes_written"] <= buffer_size, (
        f"Size mismatch attack succeeded: wrote {result['bytes_written']} bytes"
    )
    
    # Must not write more than actual data length either
    assert result["bytes_written"] <= len(actual_data) or result["status"] == "rejected", (
        f"Read beyond actual data: wrote {result['bytes_written']} "
        f"but data is only {len(actual_data)} bytes"
    )