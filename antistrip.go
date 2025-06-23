package anistrip

import (
	"bytes"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ANSI parser states
const (
	stateNormal = iota
	stateEscape // Received \x1b
	stateCSI    // Received \x1b[
)

// Writer is a thread-safe, ANSI-aware, buffering io.Writer.
type Writer struct {
	downstream        io.Writer
	inactivityTimeout time.Duration
	forceFlushTimeout time.Duration
	writeRequests     chan []byte
	caretMoved        chan struct{}
	output            chan []byte
	wg                sync.WaitGroup
	buffer            [][]rune
	cursorX           int
	cursorY           int
	flushedLines      int
	parserState       int
	params            []string
	currentParam      strings.Builder
}

// NewWriter creates and starts a new ANSI-aware buffering Writer.
func NewWriter(downstream io.Writer, inactivityTimeout, forceFlushTimeout time.Duration) *Writer {
	w := &Writer{
		downstream:        downstream,
		inactivityTimeout: inactivityTimeout,
		forceFlushTimeout: forceFlushTimeout,
		writeRequests:     make(chan []byte, 256),
		caretMoved:        make(chan struct{}, 128),
		output:            make(chan []byte, 256),
		buffer:            [][]rune{make([]rune, 0, 80)},
	}
	w.wg.Add(2)
	go w.manager()
	go w.writerLoop()
	return w
}

// writerLoop's only job is to write to the downstream writer.
func (w *Writer) writerLoop() {
	defer w.wg.Done()
	log.Printf("[WRITER]  | Starting loop")
	for p := range w.output {
		w.downstream.Write(p)
	}
	log.Printf("[WRITER]  | Loop finished.")
}

// manager owns and manages all internal state.
func (w *Writer) manager() {
	defer w.wg.Done()
	defer close(w.output)
	log.Println("[MANAGER] | Starting loop")

	var unprocessedBytes []byte
	running := true

	var inactivityTimer *time.Timer
	if w.inactivityTimeout > 0 {
		inactivityTimer = time.NewTimer(w.inactivityTimeout)
		if !inactivityTimer.Stop() {
			<-inactivityTimer.C
		}
	}
	var forceFlushTicker *time.Ticker
	if w.forceFlushTimeout > 0 {
		forceFlushTicker = time.NewTicker(w.forceFlushTimeout)
		defer forceFlushTicker.Stop()
	}
	resetInactivity := func() {
		if inactivityTimer != nil {
			if !inactivityTimer.Stop() {
				select {
				case <-inactivityTimer.C:
				default:
				}
			}
			inactivityTimer.Reset(w.inactivityTimeout)
		}
	}
	resetForceFlush := func() {
		if forceFlushTicker != nil {
			forceFlushTicker.Reset(w.forceFlushTimeout)
		}
	}
	var forceFlushC <-chan time.Time

	// This is the main event loop. It will continue to run as long as the writer is `running`
	// OR there is still data in the internal buffer left to process.
	for running || len(unprocessedBytes) > 0 {
		// First, if there's work to do, process a small chunk of it.
		// This is non-blocking and ensures the loop remains responsive.
		if len(unprocessedBytes) > 0 {
			var processedCount int
			unprocessedBytes, processedCount = w.processChunk(unprocessedBytes, 512) // Process up to 512 runes
			if processedCount == 0 && len(unprocessedBytes) > 0 {
				log.Println("[MANAGER] | WARN: Made no progress processing buffer. Discarding to prevent infinite loop.")
				unprocessedBytes = nil // Failsafe
			}
		}

		// We only enter the blocking select if we are running and have no work to do.
		// If we are shutting down (`running == false`), we skip the select and just
		// keep processing the `unprocessedBytes` buffer.
		if !running || len(unprocessedBytes) > 0 {
			// If we are shutting down OR we still have work to do,
			// we must not block. We only check for new data non-blockingly.
			select {
			case p, ok := <-w.writeRequests:
				if !ok {
					running = false
				} else {
					unprocessedBytes = append(unprocessedBytes, p...)
				}
			default:
				// No new data, just continue the loop to process the next chunk.
			}
			continue
		}

		// If we are here, it means running is true AND unprocessedBytes is empty.
		// We can now safely block and wait for an event.
		if forceFlushTicker != nil {
			forceFlushC = forceFlushTicker.C
		}
		select {
		case p, ok := <-w.writeRequests:
			if !ok {
				running = false // Signal to start shutting down.
				log.Println("[MANAGER] | Event: writeRequests channel closed.")
			} else {
				log.Printf("[MANAGER] | Event: Got %d bytes from writeRequests.", len(p))
				unprocessedBytes = append(unprocessedBytes, p...)
			}
		case <-w.caretMoved:
			resetInactivity()
		case <-inactivityTimer.C:
			log.Println("[MANAGER] | Event: Inactivity timer fired.")
			if w.flushInternal(false) {
				resetForceFlush()
			}
		case <-forceFlushC:
			log.Println("[MANAGER] | Event: Force flush ticker fired.")
			if w.flushInternal(false) {
				resetForceFlush()
			}
		}
	}

	// The loop has exited, meaning `running` is false and `unprocessedBytes` is empty.
	// All data has been processed. Perform the final guaranteed flush.
	log.Println("[MANAGER] | Loop finished. Performing final flush.")
	w.flushInternal(true)
}

// Write sends data to the manager. It is safe for concurrent use.
func (w *Writer) Write(p []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = io.ErrClosedPipe
		}
	}()
	pcopy := make([]byte, len(p))
	copy(pcopy, p)
	w.writeRequests <- pcopy
	return len(p), nil
}

// Close gracefully shuts down the writer, ensuring all buffered data is flushed.
func (w *Writer) Close() error {
	log.Println("[CLOSE]   | Close() called. Closing writeRequests.")
	close(w.writeRequests)
	w.wg.Wait()
	log.Println("[CLOSE]   | WaitGroup finished. Close complete.")

	// Final diagnostic requested by user.
	if w.flushedLines < len(w.buffer) {
		log.Printf("[CLOSE]   | DIAGNOSTIC: %d lines were left in the buffer and not flushed.", len(w.buffer)-w.flushedLines)
		for i := w.flushedLines; i < len(w.buffer); i++ {
			log.Printf("[CLOSE]   |   Unflushed Line %d: %s", i, string(w.buffer[i]))
		}
	} else {
		log.Printf("[CLOSE]   | DIAGNOSTIC: All buffer lines were flushed.")
	}

	if closer, ok := w.downstream.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (w *Writer) flushInternal(flushAll bool) bool {
	lastLineToFlush := w.cursorY
	if flushAll {
		lastLineToFlush = len(w.buffer)
	}
	if lastLineToFlush <= w.flushedLines {
		return false
	}
	var toWrite bytes.Buffer
	for i := w.flushedLines; i < lastLineToFlush; i++ {
		line := strings.TrimRight(string(w.buffer[i]), " \x00")
		toWrite.WriteString(line)
		toWrite.WriteRune('\n')
	}
	if toWrite.Len() > 0 {
		outputBytes := make([]byte, toWrite.Len())
		copy(outputBytes, toWrite.Bytes())
		select {
		case w.output <- outputBytes:
			w.flushedLines = lastLineToFlush
		default:
			log.Printf("[FLUSH]   | DROPPED FRAME: output channel is full. Downstream is likely blocked.")
		}
		return true
	}
	return false
}

// processChunk processes up to maxRunes from the data buffer and returns the remainder.
func (w *Writer) processChunk(data []byte, maxRunes int) (remaining []byte, processedRunes int) {
	bytesProcessed := 0
	for processedRunes < maxRunes && bytesProcessed < len(data) {
		r, size := utf8.DecodeRune(data[bytesProcessed:])
		if r == utf8.RuneError {
			bytesProcessed++ // Skip invalid byte
			continue
		}
		switch w.parserState {
		case stateNormal:
			w.handleNormalRune(r)
		case stateEscape:
			w.handleEscapeRune(r)
		case stateCSI:
			w.handleCSIRune(r)
		}
		bytesProcessed += size
		processedRunes++
	}
	return data[bytesProcessed:], processedRunes
}
func (w *Writer) handleNormalRune(r rune) {
	isCaretMovement := false
	switch r {
	case '\x1b':
		w.parserState = stateEscape
	case '\n':
		w.cursorY++
		w.ensureBuffer(w.cursorY, w.cursorX)
	case '\r':
		w.cursorX = 0
		isCaretMovement = true
	case '\b':
		if w.cursorX > 0 {
			w.cursorX--
		}
		isCaretMovement = true
	default:
		w.ensureBuffer(w.cursorY, w.cursorX)
		w.buffer[w.cursorY][w.cursorX] = r
		w.cursorX++
	}
	if isCaretMovement {
		select {
		case w.caretMoved <- struct{}{}:
		default:
		}
	}
}
func (w *Writer) handleEscapeRune(r rune) {
	if r == '[' {
		w.parserState = stateCSI
		w.params = w.params[:0]
		w.currentParam.Reset()
	} else {
		w.parserState = stateNormal
	}
}
func (w *Writer) handleCSIRune(r rune) {
	if (r >= '0' && r <= '9') || r == ';' {
		if r == ';' {
			w.params = append(w.params, w.currentParam.String())
			w.currentParam.Reset()
		} else {
			w.currentParam.WriteRune(r)
		}
		return
	}
	w.params = append(w.params, w.currentParam.String())
	w.currentParam.Reset()
	isCaretMovement := true
	switch r {
	case 'A', 'B', 'C', 'D':
		w.moveCursor(r)
	case 'H', 'f':
		w.setCursorPos()
	case 'J':
		w.eraseInDisplay()
	case 'K':
		w.eraseInLine()
	case 'm':
		isCaretMovement = false
	default:
		isCaretMovement = false
	}
	if isCaretMovement {
		select {
		case w.caretMoved <- struct{}{}:
		default:
		}
	}
	w.parserState = stateNormal
}
func (w *Writer) parseInt(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return val
}
func (w *Writer) moveCursor(cmd rune) {
	n := w.parseInt(w.params[0], 1)
	dx, dy := 0, 0
	switch cmd {
	case 'A':
		dy = -1
	case 'B':
		dy = 1
	case 'C':
		dx = 1
	case 'D':
		dx = -1
	}
	newCursorY := w.cursorY + dy*n
	newCursorX := w.cursorX + dx*n
	if newCursorX < 0 {
		newCursorX = 0
	}
	if newCursorY < 0 {
		newCursorY = 0
	}
	if newCursorY < w.flushedLines {
		w.flushedLines = newCursorY
	}
	w.cursorX = newCursorX
	w.cursorY = newCursorY
	w.ensureBuffer(w.cursorY, 0)
}
func (w *Writer) setCursorPos() {
	row := w.parseInt(w.params[0], 1) - 1
	col := 0
	if len(w.params) > 1 {
		col = w.parseInt(w.params[1], 1) - 1
	}
	if row < 0 {
		row = 0
	}
	if col < 0 {
		col = 0
	}
	if row < w.flushedLines {
		w.flushedLines = row
	}
	w.cursorY = row
	w.cursorX = col
	w.ensureBuffer(w.cursorY, w.cursorX)
}
func (w *Writer) eraseInLine() {
	mode := w.parseInt(w.params[0], 0)
	w.ensureBuffer(w.cursorY, w.cursorX)
	switch mode {
	case 0:
		for i := w.cursorX; i < len(w.buffer[w.cursorY]); i++ {
			w.buffer[w.cursorY][i] = ' '
		}
	case 1:
		for i := 0; i <= w.cursorX && i < len(w.buffer[w.cursorY]); i++ {
			w.buffer[w.cursorY][i] = ' '
		}
	case 2:
		for i := range w.buffer[w.cursorY] {
			w.buffer[w.cursorY][i] = ' '
		}
		w.cursorX = 0
	}
}
func (w *Writer) eraseInDisplay() {
	if mode := w.parseInt(w.params[0], 0); mode == 2 {
		w.buffer = w.buffer[:0]
		w.flushedLines, w.cursorX, w.cursorY = 0, 0, 0
		w.ensureBuffer(0, 0)
	}
}
func (w *Writer) ensureBuffer(y, x int) {
	for len(w.buffer) <= y {
		w.buffer = append(w.buffer, make([]rune, 0, 80))
	}
	for len(w.buffer[y]) <= x {
		w.buffer[y] = append(w.buffer[y], ' ')
	}
}
