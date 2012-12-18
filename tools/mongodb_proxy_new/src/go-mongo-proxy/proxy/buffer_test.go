package proxy

import (
	"fmt"
	"testing"
)

func TestReadWriteBuffer(t *testing.T) {
	buffer := NewBuffer(10)
	copy(buffer.Cursor(), []byte("helloworld"))
	buffer.ForwardCursor(len("helloworld"))
	if buffer.RemainSpace() != 0 {
		t.Errorf("Buffer write error.\n")
	} else {
		var data string
		data = fmt.Sprintf("%s", buffer.Data())
		fmt.Printf("The buffer data is %v.\n", data)
		if data != "helloworld" {
			t.Errorf("Buffer read error.\n")
		}
	}
	buffer.ResetCursor()
	copy(buffer.LimitedCursor(5), []byte("worldhello"))
	buffer.ForwardCursor(5)
	partial := fmt.Sprintf("%s", buffer.Data())
	fmt.Printf("The buffer data is %v.\n", partial)
	if partial != "world" {
		t.Errorf("Buffer read error.\n")
	}
}

func TestResetBuffer(t *testing.T) {
	buffer := NewBuffer(10)
	buffer.ForwardCursor(len("helloworld"))
	buffer.ResetCursor()
	if buffer.RemainSpace() != 10 {
		t.Errorf("Buffer cursor error.\n")
	}
}
