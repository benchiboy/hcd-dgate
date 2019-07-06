// rcs_contract_mgr project main.go
package main

import (
	"log"
)

func softwareCrc32(buf []byte, len int) int {
	bytes := make([]byte, 0)
	if len%4 != 0 {
		totalLen := (4 - len%4) + len
		bytes = make([]byte, totalLen)
		copy(bytes, buf)
		len = totalLen
	} else {
		bytes = buf
	}
	POLY := 0x04C11DB7
	crc := 0xFFFFFFFF
	arrnum := 0
	checklen := len / 4
	arr32 := make([]int, checklen)
	for i := 0; i < len; i += 4 {
		t := 0x00000000
		t = t | int(bytes[i]&0xff)
		t = t | (int(bytes[i+1]&0xff) << 8)
		t = t | (int(bytes[i+2]&0xff) << 16)
		t = t | (int(bytes[i+3]&0xff) << 24)
		arr32[arrnum] = t
		arrnum = arrnum + 1
	}
	index := 0
	for ; checklen > 0; checklen-- {
		crc = crc ^ arr32[index]
		index = index + 1
		for i := 0; i < 32; i++ {
			if (crc & 0x80000000) != 0 {
				crc = (crc << 1) ^ POLY
			} else {
				crc <<= 1
			}
		}
		crc &= 0xFFFFFFFF
	}
	return crc
}

func PrintHead(a ...interface{}) {
	log.Println("========》", a)
}

func PrintTail(a ...interface{}) {
	log.Println("《========", a)
}
