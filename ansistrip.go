package ansistrip

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

// Logger instance for trace logging
var logger = logrus.New()

// byteSlicePool helps reuse byte slices to reduce allocation pressure.
var byteSlicePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func init() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
}

// SetLogLevel allows setting the log level for the ansistrip package.
func SetLogLevel(level logrus.Level) {
	logger.SetLevel(level)
}

// ANSI parser states
const (
	stateNormal = iota
	stateEscape
	stateCSI
)

// line represents a single line in the buffer, tracking both its
// rune content and its byte size for efficient memory management.
type line struct {
	runes    []rune
	byteSize int
}

// Writer is a thread-safe, ANSI-aware, buffering io.Writer.
type Writer struct {
	downstream        io.Writer
	inactivityTimeout time.Duration
	forceFlushTimeout time.Duration
	maxBufferSize     int
	writeRequests     chan *[]byte
	caretMoved        chan struct{}
	output            chan []byte
	wg                sync.WaitGroup

	// All state below is owned exclusively by the manager goroutine.
	buffer            *btree.Map[int, *line]
	currentBufferSize int
	cursorX           int
	cursorY           int
	flushedLinesCount int
	parserState       int
	params            []string
	currentParam      strings.Builder
	csiBuffer         bytes.Buffer
}

// NewWriter creates and starts a new ANSI-aware buffering Writer.
// maxBufferSize sets the maximum size of the internal buffer in bytes.
// If the buffer exceeds this size, a flush is triggered. It also sets the
// maximum size of any single write to the downstream writer. A value of 0 or less disables this feature.
func NewWriter(downstream io.Writer, inactivityTimeout, forceFlushTimeout time.Duration, maxBufferSize int) *Writer {
	w := &Writer{
		downstream:        downstream,
		inactivityTimeout: inactivityTimeout,
		forceFlushTimeout: forceFlushTimeout,
		maxBufferSize:     maxBufferSize,
		writeRequests:     make(chan *[]byte, 256),
		caretMoved:        make(chan struct{}, 128),
		output:            make(chan []byte, 256),
		buffer:            btree.NewMap[int, *line](32),
	}
	w.wg.Add(2)
	go w.manager()
	go w.writerLoop()
	return w
}

// writerLoop's only job is to write to the downstream writer.
func (w *Writer) writerLoop() {
	defer w.wg.Done()
	logger.Tracef("[WRITER]  | Starting loop")
	for p := range w.output {
		if _, err := w.downstream.Write(p); err != nil {
			logger.Errorf("[WRITER]  | Error writing to downstream: %v", err)
		}
	}
	logger.Tracef("[WRITER]  | Loop finished.")
}

// manager owns and manages all internal state.
func (w *Writer) manager() {
	defer w.wg.Done()
	defer w.flushIncompleteSequence()
	defer close(w.output)
	logger.Trace("[MANAGER] | Starting loop")

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

	for running || len(unprocessedBytes) > 0 {
		if len(unprocessedBytes) > 0 {
			processedCount := w.processBytes(unprocessedBytes)
			unprocessedBytes = unprocessedBytes[processedCount:]

			// Check if buffer size has been exceeded and trigger a flush.
			if w.maxBufferSize > 0 && w.currentBufferSize > w.maxBufferSize {
				logger.Tracef("[MANAGER] | Buffer size %d exceeds max %d, triggering flush.", w.currentBufferSize, w.maxBufferSize)
				if w.flushInternal(false) {
					resetForceFlush()
				}
			}
		}

		if !running || len(unprocessedBytes) > 0 {
			select {
			case p, ok := <-w.writeRequests:
				if !ok {
					running = false
				} else {
					unprocessedBytes = append(unprocessedBytes, (*p)...)
					byteSlicePool.Put(p)
				}
			default:
			}
			continue
		}

		var inactivityC <-chan time.Time
		if inactivityTimer != nil {
			inactivityC = inactivityTimer.C
		}
		if forceFlushTicker != nil {
			forceFlushC = forceFlushTicker.C
		}

		select {
		case p, ok := <-w.writeRequests:
			if !ok {
				running = false
				logger.Trace("[MANAGER] | Event: writeRequests channel closed.")
			} else {
				logger.Tracef("[MANAGER] | Event: Got %d bytes from writeRequests.", len(*p))
				unprocessedBytes = append(unprocessedBytes, (*p)...)
				byteSlicePool.Put(p)
			}
		case <-w.caretMoved:
			logger.Trace("[MANAGER] | Event: Caret moved.")
			resetInactivity()
		case <-inactivityC:
			logger.Trace("[MANAGER] | Event: Inactivity timer fired.")
			if w.flushInternal(false) {
				resetForceFlush()
			}
		case <-forceFlushC:
			logger.Trace("[MANAGER] | Event: Force flush ticker fired.")
			if w.flushInternal(false) {
				resetForceFlush()
			}
		}
	}

	logger.Trace("[MANAGER] | Loop finished. Performing final flush.")
	w.flushInternal(true)
}

// Write sends data to the manager for processing.
func (w *Writer) Write(p []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = io.ErrClosedPipe
		}
	}()
	buf := byteSlicePool.Get().(*[]byte)
	*buf = append((*buf)[:0], p...)
	w.writeRequests <- buf
	return len(p), nil
}

// Close gracefully shuts down the writer.
func (w *Writer) Close() error {
	logger.Trace("[CLOSE]   | Close() called. Closing writeRequests.")
	close(w.writeRequests)
	w.wg.Wait()
	logger.Trace("[CLOSE]   | WaitGroup finished. Close complete.")
	if closer, ok := w.downstream.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// flushInternal writes lines from the buffer to the output channel.
func (w *Writer) flushInternal(flushAll bool) bool {
	var anythingFlushed bool

	// Define flushCandidate once at the top level of the function scope.
	type flushCandidate struct {
		y    int
		info *line
	}

	// Phase 1: Collect all lines that are eligible for flushing.
	var candidates []flushCandidate
	w.buffer.Scan(func(y int, lineInfo *line) bool {
		// On final flush, collect everything. Otherwise, only completed lines.
		if flushAll || y < w.cursorY {
			candidates = append(candidates, flushCandidate{y: y, info: lineInfo})
		}
		return true
	})

	// Phase 2: Process the candidates with proper batching and chunking.
	if len(candidates) > 0 {
		anythingFlushed = true

		var toWrite bytes.Buffer
		keysInBatch := make([]int, 0, len(candidates))
		var batchSize int
		var runeBuf [utf8.UTFMax]byte

		commitCurrentBatch := func() {
			if toWrite.Len() == 0 {
				return
			}

			logger.Tracef("[FLUSH]   | Committing batch of %d bytes, %d lines.", toWrite.Len(), len(keysInBatch))
			outputBytes := make([]byte, toWrite.Len())
			copy(outputBytes, toWrite.Bytes())
			w.output <- outputBytes

			for _, y := range keysInBatch {
				w.buffer.Delete(y)
			}
			w.currentBufferSize -= batchSize
			w.flushedLinesCount += len(keysInBatch)

			toWrite.Reset()
			keysInBatch = keysInBatch[:0]
			batchSize = 0
		}

		for _, cand := range candidates {
			lineRunes := cand.info.runes
			lastChar := len(lineRunes) - 1
			for lastChar >= 0 && (lineRunes[lastChar] == ' ' || lineRunes[lastChar] == '\x00') {
				lastChar--
			}

			// Estimate the size of the line to be written. This is the crucial check for batching.
			// It must be calculated before we start writing to `toWrite`.
			lineWriteSize := 0
			for i := 0; i <= lastChar; i++ {
				lineWriteSize += utf8.RuneLen(lineRunes[i])
			}
			lineWriteSize++ // For the newline

			// If adding this *entire* next line would overflow the current batch, flush the batch first.
			if w.maxBufferSize > 0 && toWrite.Len() > 0 && toWrite.Len()+lineWriteSize > w.maxBufferSize {
				commitCurrentBatch()
			}

			// Now, add the line to the buffer. This loop also handles chunking if the line
			// itself is larger than maxBufferSize.
			for i := 0; i <= lastChar; i++ {
				r := lineRunes[i]
				runeLen := utf8.RuneLen(r)
				if w.maxBufferSize > 0 && toWrite.Len() > 0 && toWrite.Len()+runeLen > w.maxBufferSize {
					// This condition is met if a single line is being chunked.
					commitCurrentBatch()
				}
				n := utf8.EncodeRune(runeBuf[:], r)
				toWrite.Write(runeBuf[:n])
			}

			// Add the newline, with a final check for overflow.
			if w.maxBufferSize > 0 && toWrite.Len() > 0 && toWrite.Len()+1 > w.maxBufferSize {
				commitCurrentBatch()
			}
			toWrite.WriteRune('\n')

			keysInBatch = append(keysInBatch, cand.y)
			batchSize += cand.info.byteSize
		}
		// After the loop, commit any remaining lines in the final batch.
		commitCurrentBatch()
	}

	// Phase 3: Handle the single-line-overflow edge case (prefix flush).
	if !flushAll && w.maxBufferSize > 0 && w.currentBufferSize > w.maxBufferSize {
		if currentLineInfo, ok := w.buffer.Get(w.cursorY); ok && len(currentLineInfo.runes) > 0 {
			anythingFlushed = true
			logger.Tracef("[FLUSH]   | Current line is causing buffer overflow. Flushing prefix.")

			var prefixSizeInBytes int
			var runeCountToFlush int
			for i, r := range currentLineInfo.runes {
				runeLen := utf8.RuneLen(r)
				if prefixSizeInBytes+runeLen > w.maxBufferSize {
					if i == 0 { // Ensure we flush at least one char, even if it's too big
						prefixSizeInBytes = runeLen
						runeCountToFlush = 1
					}
					break
				}
				prefixSizeInBytes += runeLen
				runeCountToFlush = i + 1
			}

			if runeCountToFlush > 0 {
				prefixRunes := currentLineInfo.runes[:runeCountToFlush]

				// Directly write the prefix without using the complex batching logic.
				logger.Tracef("[FLUSH]   | Committing prefix of %d bytes.", prefixSizeInBytes)
				w.output <- []byte(string(prefixRunes)) // A single allocation here is acceptable for this rare edge case.

				// Update state
				w.currentBufferSize -= prefixSizeInBytes
				currentLineInfo.runes = currentLineInfo.runes[runeCountToFlush:]
				currentLineInfo.byteSize -= prefixSizeInBytes
				w.cursorX -= runeCountToFlush
				if w.cursorX < 0 {
					w.cursorX = 0
				}
			}
		}
	}

	if anythingFlushed {
		logger.Tracef("[FLUSH]   | Flush complete. Total flushed lines: %d. New buffer size: %d.", w.flushedLinesCount, w.currentBufferSize)
	}
	return anythingFlushed
}

// processBytes processes the given byte slice.
func (w *Writer) processBytes(data []byte) (bytesProcessed int) {
	for bytesProcessed < len(data) {
		r, size := utf8.DecodeRune(data[bytesProcessed:])
		if r == utf8.RuneError {
			bytesProcessed++
			continue
		}

		if w.parserState != stateNormal && w.csiBuffer.Len() > 0 && r < 0x20 {
			w.flushIncompleteSequence()
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
	}
	return bytesProcessed
}

func (w *Writer) handleNormalRune(r rune) {
	isCaretMovement := false
	switch r {
	case '\x1b':
		w.parserState = stateEscape
		w.csiBuffer.WriteByte('\x1b')
	case '\n':
		w.cursorY++
		w.cursorX = 0
	case '\r':
		w.cursorX = 0
		isCaretMovement = true
	case '\b':
		if w.cursorX > 0 {
			w.cursorX--
		}
		isCaretMovement = true
	default:
		if r >= ' ' {
			lineInfo, _ := w.buffer.Get(w.cursorY)
			if lineInfo == nil {
				lineInfo = &line{runes: make([]rune, 0, 80)}
			}

			oldLineSize := lineInfo.byteSize

			// Ensure line is long enough, filling with spaces
			for len(lineInfo.runes) <= w.cursorX {
				lineInfo.runes = append(lineInfo.runes, ' ')
				lineInfo.byteSize++ // Space is 1 byte in UTF-8
			}

			// Replace the rune at cursorX
			oldRune := lineInfo.runes[w.cursorX]
			lineInfo.runes[w.cursorX] = r

			// Update line size based on the change
			lineInfo.byteSize -= utf8.RuneLen(oldRune)
			lineInfo.byteSize += utf8.RuneLen(r)

			w.buffer.Set(w.cursorY, lineInfo)

			// Update total buffer size
			w.currentBufferSize += (lineInfo.byteSize - oldLineSize)
			w.cursorX++
		}
	}
	if isCaretMovement {
		w.signalCaretMoved()
	}
}

func (w *Writer) handleEscapeRune(r rune) {
	w.csiBuffer.WriteRune(r)
	if r == '[' {
		w.parserState = stateCSI
		w.params = w.params[:0]
		w.currentParam.Reset()
	} else {
		w.flushIncompleteSequence()
	}
}

func (w *Writer) handleCSIRune(r rune) {
	w.csiBuffer.WriteRune(r)

	isParamChar := (r >= '0' && r <= '9') || r == ';'
	isPrivateModePrefix := (r == '?') && w.currentParam.Len() == 0 && len(w.params) == 0

	if isParamChar || isPrivateModePrefix {
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
	w.parserState = stateNormal
	w.csiBuffer.Reset()

	isCaretMovement := true
	switch r {
	case 'A', 'B', 'C', 'D':
		w.moveCursor(r)
	case 'G':
		w.setCursorColumn()
	case 'H', 'f':
		w.setCursorPos()
	case 'J':
		w.eraseInDisplay()
	case 'K':
		w.eraseInLine()
	case 'h', 'l':
		isCaretMovement = false
	case 'm':
		isCaretMovement = false
	default:
		logger.Warnf("[PROCESS] | Unsupported CSI command: '%c' with params %v", r, w.params)
		isCaretMovement = false
	}

	if isCaretMovement {
		w.signalCaretMoved()
	}
}

func (w *Writer) flushIncompleteSequence() {
	if w.csiBuffer.Len() == 0 {
		return
	}
	logger.Tracef("[PROCESS] | Flushing incomplete ANSI sequence as literal: %q", w.csiBuffer.String())
	for _, r := range w.csiBuffer.String() {
		w.handleNormalRune(r)
	}
	w.csiBuffer.Reset()
	w.parserState = stateNormal
}

func (w *Writer) parseInt(s string, defaultVal int) int {
	s = strings.TrimPrefix(s, "?")
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
	switch cmd {
	case 'A':
		w.cursorY -= n
	case 'B':
		w.cursorY += n
	case 'C':
		w.cursorX += n
	case 'D':
		w.cursorX -= n
	}
	if w.cursorX < 0 {
		w.cursorX = 0
	}
	if w.cursorY < 0 {
		w.cursorY = 0
	}
}

func (w *Writer) setCursorColumn() {
	col := w.parseInt(w.params[0], 1) - 1
	if col < 0 {
		col = 0
	}
	w.cursorX = col
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
	w.cursorY = row
	w.cursorX = col
}

func (w *Writer) eraseInLine() {
	mode := w.parseInt(w.params[0], 0)
	lineInfo, ok := w.buffer.Get(w.cursorY)
	if !ok {
		return
	}

	oldSize := lineInfo.byteSize

	switch mode {
	case 0: // Erase from cursor to end of line
		if w.cursorX < len(lineInfo.runes) {
			var removedSize int
			for _, r := range lineInfo.runes[w.cursorX:] {
				removedSize += utf8.RuneLen(r)
			}
			lineInfo.runes = lineInfo.runes[:w.cursorX]
			lineInfo.byteSize -= removedSize
		}
	case 1: // Erase from start of line to cursor
		if w.cursorX < len(lineInfo.runes) {
			sizeDelta := 0
			// Erase up to and including the cursor position
			eraseUpTo := w.cursorX
			if eraseUpTo >= len(lineInfo.runes) {
				eraseUpTo = len(lineInfo.runes) - 1
			}
			for i := 0; i <= eraseUpTo; i++ {
				sizeDelta -= utf8.RuneLen(lineInfo.runes[i])
				lineInfo.runes[i] = ' '
				sizeDelta += 1 // ' ' is 1 byte
			}
			lineInfo.byteSize += sizeDelta
		} else {
			// Cursor is at or past the end of the line, clear the whole line by replacing with spaces
			sizeDelta := 0
			for i := 0; i < len(lineInfo.runes); i++ {
				sizeDelta -= utf8.RuneLen(lineInfo.runes[i])
				lineInfo.runes[i] = ' '
				sizeDelta += 1
			}
			lineInfo.byteSize += sizeDelta
		}
	case 2: // Erase entire line
		lineInfo.runes = lineInfo.runes[:0]
		lineInfo.byteSize = 0
	}

	w.currentBufferSize += (lineInfo.byteSize - oldSize)
}

func (w *Writer) eraseInDisplay() {
	mode := w.parseInt(w.params[0], 0)
	switch mode {
	case 0: // Erase from cursor down
		w.eraseInLine()
		var linesToDelete []int
		var sizeToDelete int
		w.buffer.Scan(func(y int, lineInfo *line) bool {
			if y > w.cursorY {
				linesToDelete = append(linesToDelete, y)
				sizeToDelete += lineInfo.byteSize
			}
			return true
		})
		for _, y := range linesToDelete {
			w.buffer.Delete(y)
		}
		w.currentBufferSize -= sizeToDelete

	case 1: // Erase from cursor up
		var linesToDelete []int
		var sizeToDelete int
		w.buffer.Scan(func(y int, lineInfo *line) bool {
			if y < w.cursorY {
				linesToDelete = append(linesToDelete, y)
				sizeToDelete += lineInfo.byteSize
				return true
			}
			return false
		})
		for _, y := range linesToDelete {
			w.buffer.Delete(y)
		}
		w.currentBufferSize -= sizeToDelete

		w.params = []string{"1"}
		w.eraseInLine()
	case 2: // Erase entire display
		w.currentBufferSize = 0
		w.buffer.Clear()
		w.cursorX, w.cursorY = 0, 0
	}
}

func (w *Writer) signalCaretMoved() {
	select {
	case w.caretMoved <- struct{}{}:
	default:
	}
}
