package proxy

import (
	"fmt"
	"testing"
)

func TestParseMsgHeader(t *testing.T) {
	invalid_msg := []byte{0x0, 0x0, 0x0, 0x0}
	pkt_len, op_code := parseMsgHeader(invalid_msg)
	if pkt_len != 0 || op_code != OP_UNKNOWN {
		t.Errorf("Failed to parse %v.\n", invalid_msg)
	} else {
		fmt.Printf("Succeed to get length %d and opcode %d.\n", pkt_len, op_code)
	}

	insert_msg := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0xd2, 0x7, 0x0, 0x0}
	pkt_len, op_code = parseMsgHeader(insert_msg)
	if pkt_len != 1 || op_code != OP_INSERT {
		t.Errorf("Failed to parse %v.\n", insert_msg)
	} else {
		fmt.Printf("Succeed to get length %d and opcode %d.\n", pkt_len, op_code)
	}

	query_msg := []byte{0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0xd4, 0x7, 0x0, 0x0}
	pkt_len, op_code = parseMsgHeader(query_msg)
	if pkt_len != 2 || op_code != OP_QUERY {
		t.Errorf("Failed to parse %v.\n", query_msg)
	} else {
		fmt.Printf("Succeed to get length %d and opcode %d.\n", pkt_len, op_code)
	}
}
