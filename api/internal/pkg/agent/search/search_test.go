package search

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

// func TestContainer_isSearchByTime(t *testing.T) {
//	obj := NewComponent("2023-08-22 23:00:00", "2023-10-01 00:00:00", "test_files/ego2.sys", "", 1)
//	flag := obj.isSearchByStartTime(`{"lv":"info","ts":"2023-09-25 11:10:25","caller":"file/file.go:97","msg":"read watch","comp":"core.econf","comp":"file datasource","configFile":"/data/config/svc-history.toml","realConfigFile":"/data/config/svc-history.toml","fppath":"/data/config/svc-history.toml"}`)
//	assert.True(t, flag)
// }
// ``

// count:  276
// from:  1695616909
// to:  1695703309
func TestOpenfile(t *testing.T) {
	startPos := 43425131
	endPos := 43463215
	file, _ := OpenFile("./100w.sys")
	file.ptr.Seek(int64(startPos), 0)
	scanner := bufio.NewScanner(file.ptr)
	endPos = endPos - startPos
	pos := 0
	count := 0
	for scanner.Scan() {
		text := scanner.Text()
		offset := bytes.Count([]byte(text), []byte{'\n'})
		pos += len([]byte(text)) + 1
		if pos > endPos {
			break
		}
		fmt.Printf("pos: %d, text: %s, newLineOffset: %d, \n", pos, text, offset)
		count += 1
	}
	fmt.Println("count: ", count)
}

func TestFindBorder(t *testing.T) {
	file, _ := OpenFile("./ego3.sys")
	// findBorder(file.ptr, 0, 1000, 1, 1024*60)
	from, to, _ := findString(file.ptr, 131, 900)
	val, _ := getString(file.ptr, from, to)
	// buf := make([]byte, 141)
	// file.ptr.Read(buf)
	// fmt.Println(string(buf))
	fmt.Println("val:", val)
}

func TestMutliSeekFile(t *testing.T) {
	file, _ := OpenFile("./ego3.sys")
	buf := make([]byte, 100)
	file.ptr.Seek(131, 0)
	file.ptr.Read(buf)
	fmt.Println("1: ", string(buf))

	file.ptr.Seek(0, 0)
	file.ptr.Read(buf)
	fmt.Println("2: ", string(buf))
}

// func TestBufCalc(t *testing.T) {
// 	file, _ := OpenFile("./ego3.sys")
// 	cnt := bufCalcLines(*file, 0, 500)
// 	fmt.Println("cnt: ", cnt)
// }
// func bufCalcLines(file File, startPos, endPos int64) int64 {
// 	B := 1024
// 	buf := make([]byte, B)
// 	st := startPos
// 	lines := int64(0)
// 	for st < endPos {
// 		file.ptr.Seek(st, 0)
// 		file.ptr.Read(buf)
// 		cnt := bytes.Count(buf, []byte{'\n'})
// 		if cnt != -1 {
// 			lines += int64(cnt)
// 		}
// 		st += int64(B)
// 	}
// 	return lines
// }

func TestSearchAlgorithm(t *testing.T) {
	comp := NewComponent("./500w.sys", Request{
		StartTime: 1697356865,
		EndTime:   1697356869,
		KeyWord:   "lv=info and comp=Timeout exceeded and etc=server.port:8080",
	})

	sp := int64(0)
	ep := comp.file.size
	now := sp
	var (
		success             = false
		existLine           = true
		filter     []string = comp.filterWords
		size                = 1024 * 1024
		lines      int64
		fileReader []byte = make([]byte, size)
		before     []byte
		beforeLen  = 0
	)
	total := int64(0)
	for {
		data := make([]byte, size+beforeLen)
		comp.file.ptr.Seek(now, 0)
		comp.file.ptr.Read(fileReader)
		if beforeLen > 0 {
			// fmt.Printf("合并之前 before: %s\n", string(before))
			comp.file.ptr.Read(data[beforeLen:])
			copy(data[:beforeLen], before)
		}
		copy(data[beforeLen:], fileReader)
		// fmt.Printf("完成： data: %s%s\n", string(data), "#123456")
		lines, filter, before = comp.algorithm(data, filter, &success, &existLine)
		total += lines
		// fmt.Println(string(data))
		// fmt.Println(lines)
		if existLine {
			filter = comp.filterWords
		}
		if before != nil {
			beforeLen = len(before)
		}
		now += int64(size)
		if now > ep {
			break
		}
		fileReader = fileReader[0:0]
	}
	fmt.Printf("total : %d \n", total)
	defer comp.file.ptr.Close()
}

func TestReadFile(t *testing.T) {
	comp := NewComponent("./1000w.sys", Request{
		StartTime: 1697356865,
		EndTime:   1697356869,
		KeyWord:   fmt.Sprintf("lv=%s and comp=%s and etc=%s", "info", "Timeout exceeded", "server.port:8080"),
	})

	buf := make([]byte, 1024+100)
	before := make([]byte, 100)
	before[99] = 99
	comp.file.ptr.Read(buf)
	copy(buf[:100], before)
	fmt.Println(buf)

}
