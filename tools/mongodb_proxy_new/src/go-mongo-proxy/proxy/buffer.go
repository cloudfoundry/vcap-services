package proxy

type Buffer interface {
	Cursor() []byte
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
