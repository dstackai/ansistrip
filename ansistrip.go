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

// Writer is a thread-safe, ANSI-aware, buffering io.Writer.
type Writer struct {
	downstream        io.Writer
	inactivityTimeout time.Duration
	forceFlushTimeout time.Duration
	writeRequests     chan *[]byte
	caretMoved        chan struct{}
	output            chan []byte
	wg                sync.WaitGroup

	// All state below is owned exclusively by the manager goroutine.
	buffer            *btree.Map[int, []rune] // Ordered sparse buffer (B-Tree)
	cursorX           int
	cursorY           int
	flushedLinesCount int
	parserState       int
	params            []string
	currentParam      strings.Builder
	csiBuffer         bytes.Buffer
}

// NewWriter creates and starts a new ANSI-aware buffering Writer.
func NewWriter(downstream io.Writer, inactivityTimeout, forceFlushTimeout time.Duration) *Writer {
	w := &Writer{
		downstream:        downstream,
		inactivityTimeout: inactivityTimeout,
		forceFlushTimeout: forceFlushTimeout,
		writeRequests:     make(chan *[]byte, 256),
		caretMoved:        make(chan struct{}, 128),
		output:            make(chan []byte, 256),
		buffer:            btree.NewMap[int, []rune](32),
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
	var toWrite bytes.Buffer
	linesToFlush := make([]int, 0, 16)

	w.buffer.Scan(func(y int, line []rune) bool {
		if y < w.cursorY || flushAll {
			toWrite.WriteString(strings.TrimRight(string(line), " \x00"))
			toWrite.WriteRune('\n')
			linesToFlush = append(linesToFlush, y)
		}
		if !flushAll && y >= w.cursorY {
			return false
		}
		return true
	})

	if toWrite.Len() > 0 {
		logger.Tracef("[FLUSH]   | Flushing %d lines.", len(linesToFlush))
		outputBytes := make([]byte, toWrite.Len())
		copy(outputBytes, toWrite.Bytes())
		w.output <- outputBytes

		for _, y := range linesToFlush {
			w.buffer.Delete(y)
		}
		w.flushedLinesCount += len(linesToFlush)
		logger.Tracef("[FLUSH]   | Flush complete. Total flushed lines: %d.", w.flushedLinesCount)
		return true
	}
	return false
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
			line, _ := w.buffer.Get(w.cursorY)
			for len(line) <= w.cursorX {
				line = append(line, ' ')
			}
			line[w.cursorX] = r
			w.buffer.Set(w.cursorY, line)
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
	line, _ := w.buffer.Get(w.cursorY)

	switch mode {
	case 0:
		if w.cursorX < len(line) {
			w.buffer.Set(w.cursorY, line[:w.cursorX])
		}
	case 1:
		if w.cursorX < len(line) {
			for i := 0; i <= w.cursorX && i < len(line); i++ {
				line[i] = ' '
			}
			w.buffer.Set(w.cursorY, line)
		} else {
			w.buffer.Set(w.cursorY, make([]rune, 0, 80))
		}
	case 2:
		w.buffer.Set(w.cursorY, make([]rune, 0, 80))
	}
}

func (w *Writer) eraseInDisplay() {
	mode := w.parseInt(w.params[0], 0)
	switch mode {
	case 0:
		w.eraseInLine()
		w.buffer.Scan(func(y int, _ []rune) bool {
			if y > w.cursorY {
				w.buffer.Delete(y)
			}
			return true
		})
	case 1:
		w.buffer.Scan(func(y int, _ []rune) bool {
			if y < w.cursorY {
				w.buffer.Delete(y)
				return true
			}
			return false
		})
		w.params = []string{"1"}
		w.eraseInLine()
	case 2:
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
