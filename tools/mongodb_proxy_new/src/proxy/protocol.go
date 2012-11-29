package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// All the constants are compatible in the following mongodb versions:
// 1.8, 2.0, 2.2
const OP_UNKNOWN = 0
const OP_REPLY = 1
const OP_MSG = 1000
const OP_UPDATE = 2001
const OP_INSERT = 2002
const RESERVED = 2003
const OP_QUERY = 2004
const OP_GETMORE = 2005
const OP_DELETE = 2006
const OP_KILL_CURSORS = 2007

const STANDARD_HEADER_SIZE = 16
const RESPONSE_HEADER_SIZE = 20

func parseMsgHeader(packet []byte) (pkt_len, op_code uint32) {
	if len(packet) < STANDARD_HEADER_SIZE {
		return 0, OP_UNKNOWN
	}

	buf := bytes.NewBuffer(packet[0:4])
	// Note that like BSON documents, all data in the mongo wire
	// protocol is little-endian.
	err := binary.Read(buf, binary.LittleEndian, &pkt_len)
	if err != nil {
		fmt.Printf("Failed to do binary read message_length [%s].\n", err)
		return 0, OP_UNKNOWN
	}

	buf = bytes.NewBuffer(packet[12:16])
	err = binary.Read(buf, binary.LittleEndian, &op_code)
	if err != nil {
		fmt.Printf("Failed to do binary read op_code [%s].\n", err)
		return 0, OP_UNKNOWN
	}

	return pkt_len, op_code
}
