# ansistrip

`ansistrip` is a buffering writer for Go that strips ANSI escape sequences (cursor movement, text styling, erase commands) while
maintaining a virtual cursor position and screen buffer.

Output is flushed when:
- Cursor doesn't move (inactivity timeout)
- A force flush timeout is reached
- The writer is closed

Use `ansistrip` if you want to run a Docker container in TTY mode and stream logs for both TTY and non-TTY modes.

## Usage

```go
package main

import (
    "os"
    "time"
    "github.com/dstackai/ansistrip"
)

func main() {
    // Create a new ANSI-aware writer
    writer := ansistrip.NewWriter(
        os.Stdout,                // downstream writer
        500*time.Millisecond,     // inactivity timeout
        3*time.Second,            // force flush timeout
    )
    defer writer.Close()

    // Write data with ANSI sequences
    writer.Write([]byte("\x1b[31mRed text\x1b[0m\n"))
    writer.Write([]byte("\x1b[1;32mBold green text\x1b[0m\n"))
}
```