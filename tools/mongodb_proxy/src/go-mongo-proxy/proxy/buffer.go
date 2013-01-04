package proxy

type Buffer interface {
	Cursor() []byte
	LimitedCursor(int) []byte
	ForwardCursor(int)
	ResetCursor()
	Data() []byte
	RemainSpace() int
}

type BufferImpl struct {
	data  []byte
	start int
	end   int
}

func (buffer *BufferImpl) Cursor() []byte {
	return buffer.data[buffer.start:buffer.end]
}

// caller must ensure buffer overflow never happen
func (buffer *BufferImpl) LimitedCursor(length int) []byte {
	end := buffer.start + length
	if end > buffer.end {
		panic("buffer overflow")
	}
	return buffer.data[buffer.start:end]
}

// caller must ensure buffer overflow never happen
func (buffer *BufferImpl) ForwardCursor(length int) {
	if buffer.start+length > buffer.end {
		panic("buffer overflow")
	}
	buffer.start += length
}

func (buffer *BufferImpl) ResetCursor() {
	buffer.start = 0
}

func (buffer *BufferImpl) Data() []byte {
	return buffer.data[0:buffer.start]
}

func (buffer *BufferImpl) RemainSpace() int {
	return buffer.end - buffer.start
}

func NewBuffer(size int) *BufferImpl {
	return &BufferImpl{
		data:  make([]byte, size),
		start: 0,
		end:   size}
}
