package search

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotomicro/ego/core/elog"
	"github.com/spf13/cast"

	"github.com/clickvisual/clickvisual/api/internal/pkg/cvdocker"
	db2 "github.com/clickvisual/clickvisual/api/internal/pkg/model/db"
	"github.com/clickvisual/clickvisual/api/internal/pkg/utils/gopool"
)

const (
	MB = 1024 * 1024
)

var (
	pool = gopool.NewPool("calc", 10, gopool.NewConfig())
)

// isSearchTime 根据时间搜索到数据
// 根据数据匹配，获得后面的时间数据，"ts":"(.*)"
// $1 拿到数据后，按照预设的时间格式解析
// startTime，数据大于他的都符合要求
// endTime，数据小于他的都符合要求
func (c *Component) isSearchByStartTime(value string) bool {
	curTime, indexValue := Index(value, `"ts":"`)
	if indexValue == -1 {
		return false
	}
	curTimeParser, err := time.ParseInLocation("2006-01-02 15:04:05", curTime, time.Local)
	if err != nil {
		panic(err)
	}
	if curTimeParser.Unix() >= c.startTime {
		return true
	}
	return false
}

func (c *Component) isSearchByEndTime(value string) bool {
	curTime, indexValue := Index(value, `"ts":"`)
	if indexValue == -1 {
		return false
	}
	curTimeParser, err := time.ParseInLocation("2006-01-02 15:04:05", curTime, time.Local)
	if err != nil {
		panic(err)
	}
	if curTimeParser.Unix() <= c.endTime {
		return true
	}
	return false
}

func (c *Component) isSearchByKeyWord(value string) bool {
	flag := true
	for _, str := range c.filterWords {
		flag = strings.Contains(value, str) && flag
	}

	return flag
}

// search returns first byte number in the ordered `file` where `pattern` is occured as a prefix string
func (c *Component) searchByStartTime() (int64, error) {
	result := int64(-1)
	from := int64(0)
	to := c.file.size - 1

	const maxCalls = 128
	currCall := 0

	for {
		if from < 0 || from > to || to >= c.file.size {
			return result, nil
		}

		if currCall > maxCalls {
			return -1, errors.New("MAX_CALLS_EXCEEDED")
		}

		// 二分法查找
		strFrom, strTo, err := findString(c.file.ptr, from, to)
		if err != nil {
			return -1, err
		}
		value, err := getString(c.file.ptr, strFrom, strTo)
		if err != nil {
			return -1, err
		}

		isSearch := c.isSearchByStartTime(value)
		// 如果查到了满足条件，继续往上一层查找
		if isSearch {
			// it's already result, but we need to search for more results
			result = strFrom
			to = strFrom - int64(1)
		} else {
			// it's not a result, we need to search for more results
			from = strTo + int64(1)
		}
		currCall++
	}
}

// search returns first byte number in the ordered `file` where `pattern` is occured as a prefix string
func (c *Component) searchByEndTime() (int64, error) {
	result := int64(-1)
	from := int64(0)
	to := c.file.size - 1

	const maxCalls = 128
	currCall := 0

	for {
		if from < 0 || from > to || to >= c.file.size {
			return result, nil
		}

		if currCall > maxCalls {
			return -1, errors.New("MAX_CALLS_EXCEEDED")
		}

		strFrom, strTo, err := findString(c.file.ptr, from, to)
		if err != nil {
			return -1, err
		}
		value, err := getString(c.file.ptr, strFrom, strTo)
		if err != nil {
			return -1, err
		}

		isSearch := c.isSearchByEndTime(value)
		if isSearch {
			// it's already result, but we need to search for more results
			result = strTo
			from = strTo + int64(2) // next byte is \n, so we need to move to the bytes after \n
		} else {
			// it's not a result, we need to search for more results
			to = strFrom - int64(1)
		}
		currCall++
	}
}

// search returns first byte number in the ordered `file` where `pattern` is occured as a prefix string
func (c *Component) searchByWord(startPos, endPos int64) (int64, error) {
	// 游标去掉一部分数据
	_, err := c.file.ptr.Seek(startPos, io.SeekStart)
	if err != nil {
		panic(err)
	}
	i := 0
	// 在读取这个内容
	scanner := bufio.NewScanner(c.file.ptr)
	for scanner.Scan() {
		// 超过位置，直接退出
		if int64(i) > endPos {
			break
		}
		i += len(scanner.Text())
		flag := c.isSearchByKeyWord(scanner.Text())
		if flag {
			str := scanner.Text()
			for _, value := range c.filterWords {
				str = c.bash.ColorWord(value, str)
			}
			fmt.Println(str)
		}
	}
	return 0, nil
}

func (c *Component) calcLines(startPos, endPos int64) int64 {
	// 游标去掉一部分数据
	_, err := c.file.ptr.Seek(startPos, io.SeekStart)
	if err != nil {
		panic(err)
	}
	i := 0
	var count int64 = 0
	endPos = endPos - startPos
	// 在读取这个内容
	scanner := bufio.NewScanner(c.file.ptr)
	// bufio.NewReaderSize(c.file.ptr, 1024*1024)
	for scanner.Scan() {
		text := scanner.Text()
		i += len(text)
		// 超过位置，直接退出
		if int64(i) > endPos {
			break
		}
		// fmt.Printf("startPos: %d, endPos: %d, text:%s\n", startPos, endPos, text)
		count++
	}
	// fmt.Printf("startPos: %d, endPos: %d, count: %d\n", startPos, endPos, count)

	return count
}

func (c *Component) calcLinesGoroutine(startPos, endPos int64) int64 {
	var count atomic.Int64
	size := endPos - startPos + 1
	var wg sync.WaitGroup
	if size >= 1.5*MB {
		st := startPos
		times := size / MB
		wg.Add(int(times))
		for st < endPos {
			et := st + MB
			if et > endPos {
				et = endPos
			}
			pool.Go(func() {
				defer wg.Done()
				cnt := c.bufCalcLines(st, et)
				count.Add(cnt)
			})
			st += MB
		}
		wg.Wait()
	} else {
		cnt := c.bufCalcLines(startPos, endPos)
		count.Add(cnt)
	}
	return count.Load()
}

func (c *Component) bufCalcLines(startPos, endPos int64) int64 {
	st := startPos
	buf := make([]byte, MB)
	lines := int64(0)
	for st < endPos {
		c.file.ptr.Seek(st, 0)
		c.file.ptr.Read(buf)
		cnt := 0
		if endPos-st < MB {
			cnt = bytes.Count(buf[:endPos-st], []byte{'\n'})
		} else {
			cnt = bytes.Count(buf, []byte{'\n'})
		}
		if cnt != -1 {
			lines += int64(cnt)
		}
		st += MB
	}
	// _, err := c.file.ptr.Seek(startPos, io.SeekStart)
	// if err != nil {
	// 	panic(err)
	// }
	// i := 0
	// endPos = endPos - startPos
	// scanner := bufio.NewScanner(c.file.ptr)
	// for scanner.Scan() {
	// 	i += len(scanner.Text())
	// 	if int64(i) >= endPos {
	// 		break
	// 	}
	// 	lines++
	// }
	return lines
}

func (c *Component) parseHitLog(line string) (log map[string]interface{}, err error) {
	log = make(map[string]interface{})
	for _, word := range c.words {
		log[word.Key] = word.Value
	}
	c.logs = append(c.logs, log)
	curTime, indexValue := Index(line, `"ts":"`)
	if indexValue == -1 {
		return
	}
	curTimeParser, err := time.ParseInLocation("2006-01-02 15:04:05", curTime, time.Local)
	if err != nil {
		elog.Error("agent log parse timestamp error", elog.FieldErr(err))
		panic(err)
	}
	ts := curTimeParser.Unix()
	if c.request.K8sClientType == cvdocker.ClientTypeContainerd {
		line = getFilterK8SContainerdWrapLog(line)
	}

	log["ts"] = ts
	log["body"] = line
	log[db2.TimeFieldNanoseconds] = curTimeParser.UnixNano()
	log[db2.TimeFieldSecond] = ts
	return
}

func (c *Component) mappingCharts(line string, charts *sync.Map) {
	curTime, indexValue := Index(line, `"ts":"`)
	if indexValue == -1 {
		return
	}
	curTimeParser, err := time.ParseInLocation("2006-01-02 15:04:05", curTime, time.Local)
	if err != nil {
		elog.Error("agent log parse timestamp error", elog.FieldErr(err))
		panic(err)
	}
	ts := curTimeParser.Unix()

	// 根据时间偏移量统计

	offset := (ts - c.startTime) / c.interval

	oldVal, ok := charts.LoadOrStore(offset, 1)
	for ok && !charts.CompareAndSwap(offset, oldVal, cast.ToInt64(oldVal)+1) {
		oldVal, ok = charts.Load(offset)
	}

	if offset > c.maxTimes {
		c.maxTimes = offset
	}
	return
}

func (c *Component) searchChartsByBackWord(startPos, endPos int64) {
	_, err := c.file.ptr.Seek(startPos, 0)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(c.file.ptr)
	end := endPos - startPos
	pos := 0
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
		str := string(line)

		pos += len(line)
		if int64(pos) > end {
			break
		}

		flag := c.isSearchByKeyWord(str)
		if flag {
			curTime, indexValue := Index(str, `"ts":"`)
			if indexValue == -1 {
				return
			}
			curTimeParser, err := time.ParseInLocation("2006-01-02 15:04:05", curTime, time.Local)
			if err != nil {
				elog.Error("agent log parse timestamp error", elog.FieldErr(err))
				panic(err)
			}
			ts := curTimeParser.Unix()

			// 根据时间偏移量统计

			offset := (ts - c.startTime) / c.interval
			c.charts[offset]++
			if offset > c.maxTimes {
				c.maxTimes = offset
			}
		}
	}
}

// algorithm 根据范围的数据,计算匹配数据行数
// data 读取的数据
// filter 需要进行匹配的数据，如果 success 为 true，那么就为原数据，用于下一行匹配
// success 上一次计算是否匹配成功
// exist 上一次是否匹配到 \n
// 若 success = false, exist = false, 即还未找到 \n, filter 为还未匹配完成的数据
// 若 success = false, exist = true，上次匹配到 \n, filter 为原数据
func (c *Component) algorithm(data []byte, filter []string, success, existLine *bool) (lines int64, filterWord []string, before []byte) {
	var (
		pos          int = -1
		ok           bool
		filterPosMap map[string]int = make(map[string]int)
		emptyPos     bool           = true
		lastPos      int
		tailLine     []byte
	)
	// 上一次没有匹配到 \n
	if !*existLine {
		pos = bytes.Index(data, []byte{'\n'})
		if pos == -1 {
			if !*success {
				_, filter, *success, _ = verifyKeyWords(data, filter, -1)
			}
			return 0, filter, data
		} else {
			if !*success {
				filterPosMap, _, ok, emptyPos = verifyKeyWords(data[:pos], filter, pos)
			}
			if ok || *success {
				lines++
			}
			data = data[pos+1:]
			pos = bytes.Index(data, []byte{'\n'})

			// 更新 pos map
			for k, _ := range filterPosMap {
				filterPosMap[k] -= (pos + 1)
			}
		}
		filter = c.filterWords
	}

	if pos == -1 {
		pos = bytes.Index(data, []byte{'\n'})
		if pos == -1 {
			_, filter, *success, _ = verifyKeyWords(data, filter, -1)
			*existLine = false
			return lines, filter, data
		}
	}

	lastPos = bytes.LastIndex(data, []byte{'\n'})
	tailLine = data[lastPos+1:]

	if lastPos == len(data)-1 {
		*existLine = true
	}

	for pos != -1 {
		skipTag := -1
		flag := true
		for _, v := range filter {
			if !flag {
				break
			}

			if !emptyPos {
				p, ok := filterPosMap[v]
				if ok {
					if p > pos {
						skipTag = pos
						flag = false
						break
					}
				}
			}

			p, ok := verifyKeyWord(data, v, pos)

			// 若读取的没有找到，说明这段数据中不可能存在匹配的日志
			// 此时需要找到末尾的 \n 重新匹配
			if !ok {
				if p == -1 {
					_, filter, *success, _ = verifyKeyWords(tailLine, c.filterWords, -1)
					return lines, filter, tailLine
				}
				filterPosMap[v] = p
				if p > skipTag {
					skipTag = p
				}
				flag = false
			}
		}

		if flag {
			// fmt.Println("lines: ", string(data[:pos]))
			lines++
			data = data[pos+1:]
			pos = bytes.Index(data, []byte{'\n'})
			continue
		}

		if !flag && skipTag != -1 {
			if skipTag <= pos {
				data = data[pos+1:]
				pos = bytes.Index(data, []byte{'\n'})
				if pos == -1 {
					_, filter, *success, _ = verifyKeyWords(tailLine, c.filterWords, -1)
					return lines, filter, tailLine
				}
				continue
			}

			for skipTag > pos {
				skipTag -= pos + 1
				data = data[pos+1:]
				pos = bytes.Index(data, []byte{'\n'})
				if pos == -1 {
					_, filter, *success, _ = verifyKeyWords(tailLine, c.filterWords, -1)
					return lines, filter, tailLine
				}
			}
		}
	}
	return lines, nil, tailLine
}

// verifyKeyWords 根据关键词进行匹配，确保位置在 pos 之内
// Return
// map[string]int: 匹配到的在 pos 之后的数据
// []string: 未匹配的 filter
// bool: 是否存在合法匹配
// bool: map 是否存在数据
func verifyKeyWords(data []byte, filter []string, pos int) (map[string]int, []string, bool, bool) {
	var (
		filterWordsToPos map[string]int = make(map[string]int)
		ok                              = true
		retFilter                       = make([]string, 0)
		emptyPos                        = true
	)
	for _, v := range filter {
		p := bytes.Index(data, []byte(v))
		if p == -1 {
			ok = false
			retFilter = append(retFilter, v)
		} else {
			filterWordsToPos[v] = p
			emptyPos = false
			if p > pos {
				ok = false
			}
		}
	}

	return filterWordsToPos, retFilter, ok, emptyPos
}

// verifyKeyWords 根据关键词进行匹配，确保位置在 pos 之内
// Return
// int: 匹配到的pos数据
// bool: 是否合法
func verifyKeyWord(data []byte, filter string, pos int) (int, bool) {
	p := bytes.Index(data, []byte(filter))
	return p, p != -1 && p < pos
}

func (c *Component) searchChartsByBackWordGoroutine(startPos, endPos int64) {

	from, to, _ := findString(c.file.ptr, startPos, endPos)

	// _, err := c.file.ptr.Seek(startPos, io.SeekStart)
	// if err != nil {
	// 	panic(err)
	// }
	// i := int64(0)
	// var (
	// 	str string
	// )

	var wg sync.WaitGroup
	var charts sync.Map

	wg.Add(2)
	calc := func(start, end int64) {
		file, err := os.OpenFile(c.file.path, os.O_RDONLY, 0777)
		defer file.Close()
		// 游标去掉一部分数据
		_, err = file.Seek(start, io.SeekStart)
		if err != nil {
			panic(err)
		}
		reader := bufio.NewReader(file)
		end = end - start
		pos := 0
		for {
			line, _, err := reader.ReadLine()
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			str := string(line)
			pos += len(line)

			if int64(pos) > end {
				break
			}
			// fmt.Println(str)
			flag := c.isSearchByKeyWord(str)
			if flag {
				c.mappingCharts(str, &charts)
			}
		}
	}

	pool.Go(func() {
		calc(startPos, from)
		wg.Done()
	})

	pool.Go(func() {
		calc(to+2, endPos)
		wg.Done()
	})

	wg.Wait()

	charts.Range(func(key, value interface{}) bool {
		k, v := cast.ToInt64(key), cast.ToInt64(value)
		// fmt.Printf("==== k: %d, v: %d ====\n", k, v)
		c.charts[k] = v
		return true
	})
	// fmt.Println(c.charts, len(c.charts))
}

// search returns first byte number in the ordered `file` where `pattern` is occured as a prefix string
func (c *Component) searchByBackWord(startPos, endPos int64) (logs []map[string]interface{}, error error) {
	// 游标去掉一部分数据
	_, err := c.file.ptr.Seek(startPos, io.SeekStart)
	if err != nil {
		panic(err)
	}
	i := int64(0)
	var (
		str string
	)
	scanner := NewBackScan(c.file.ptr, c.file.size)
	for {
		line, _, err := scanner.Line()
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
		if len(c.filterWords) > 0 {
			flag := c.isSearchByKeyWord(line)
			if flag {
				str = line
				if c.request.IsCommand {
					for _, value := range c.filterWords {
						str = c.bash.ColorWord(value, str)
					}
					if c.request.K8sClientType == cvdocker.ClientTypeContainerd {
						str = getFilterK8SContainerdWrapLog(str)
					}
					c.output = append(c.output, str)
				} else {
					_, err := c.parseHitLog(str)
					if err != nil {
						return nil, err
					}
				}

				if i == c.limit {
					break
				}
				i++
			}
		} else {
			c.output = append(c.output, line)
			if i == c.limit {
				break
			}
			i++
		}
	}
	return c.logs, nil
}

// search returns first byte number in the ordered `file` where `pattern` is occured as a prefix string
func (c *Component) searchByWord2(startPos, endPos int64) (int64, error) {
	var err error
	var cursor = startPos
	buff := make([]byte, 0, 4096)
	char := make([]byte, 1)
	cnt := 0
	// scanner := bufio.NewReader(c.file.ptr.)
	// scanner.ReadString("/n")
	for {
		_, _ = c.file.ptr.Seek(cursor, io.SeekStart)
		_, err = c.file.ptr.Read(char)
		if err != nil {
			panic(err)
		}

		if char[0] == '\n' {
			if len(buff) > 0 {
				// 读取到的行
				flag := c.isSearchByKeyWord(string(buff))
				if flag {
					fmt.Println(string(buff))
				}
				cnt++
				if cnt == 1000000 {
					// 超过数量退出
					break
				}

			}
			buff = buff[:0]
		} else {
			buff = append(buff, char[0])
		}

		if cursor == endPos {
			break
		}
		cursor++
	}
	return 0, nil
}

// min returns minimum of two int64 numbers
func min(a int64, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

// writeBytes writes [start; stop] bytes from fromFile to toFile
func writeBytes(fromFile *os.File, start int64, stop int64, toFile *os.File, maxBufferSize int64) (int64, error) {
	var bytesWritten int64
	bytesWritten = 0
	if start > stop {
		return bytesWritten, nil
	}

	fromFile.Seek(start, 0)
	buffer := make([]byte, min(stop-start+1, maxBufferSize))
	for current := start; current < stop; {
		bufferSize := min(stop-current+1, maxBufferSize)
		if bufferSize < maxBufferSize {
			buffer = make([]byte, bufferSize)
		}

		n, err := fromFile.Read(buffer)
		if err != nil {
			return bytesWritten, err
		} else if int64(n) < bufferSize {
			return bytesWritten, errors.New("Error: unexpected end of input")
		}
		n, err = toFile.Write(buffer)
		if err != nil {
			return bytesWritten, err
		}
		bytesWritten += int64(n)

		current += int64(bufferSize)
	}

	return bytesWritten, nil
}

// newLineIndex returns index of newline symbol in buffer;
// if no newline symbol found returns -1
func newLineIndex(buffer []byte, diff int64) int {
	n := len(buffer)
	if n == 0 {
		return -1
	}
	idx := 0
	if diff == -1 {
		idx = n - 1
	}

	for {
		if n == 0 {
			return -1
		}

		if buffer[idx] == '\n' {
			return idx
		}
		idx = idx + int(diff)
		n--
	}
}

// findBorder searches for newline symbol in [from; to]
// when diff = 1 makes forward search (`from` -> `to`)
// when diff = -1 makes backward search (`to` -> `from`)
func findBorder(file *os.File, from int64, to int64, diff int64, maxBufferSize int64) (int64, error) {
	size := to - from + int64(1)
	currentSize := min(size, maxBufferSize)

	position := from
	if diff == -1 {
		position = to - currentSize + int64(1)
	}
	buffer := make([]byte, currentSize)

	for {
		if size == 0 {
			return -1, nil
		}
		if int64(len(buffer)) != currentSize {
			buffer = make([]byte, currentSize)
		}

		file.Seek(position, 0)

		n, err := file.Read(buffer)
		// fmt.Println("buffer : ", string(buffer))
		if err != nil {
			return -1, err
		} else if int64(n) < currentSize {
			return -1, errors.New("Error: unexpected end of input")
		}

		idx := newLineIndex(buffer, diff)
		if idx >= 0 {
			// fmt.Println("newLineIndex : ", position+int64(idx))
			return position + int64(idx), nil
		}

		position = position + diff*currentSize
		size = size - currentSize
		currentSize = min(size, maxBufferSize)
	}
}

// findString searches string borders
// returns (leftBorder, rightBorder, error)
func findString(file *os.File, from int64, to int64) (int64, int64, error) {
	maxBufferSize := int64(60 * 1024)
	middle := (from + to) / 2
	strFrom, err := findBorder(file, from, middle, -1, maxBufferSize)
	if err != nil {
		return -1, -1, err
	} else if strFrom == -1 {
		// no newline found, just return from position
		strFrom = from
	} else {
		// new line found, need to increment position to omit newline byte
		strFrom++
	}
	strTo, err := findBorder(file, middle+1, to, 1, maxBufferSize)
	if err != nil {
		return -1, -1, err
	} else if strTo == -1 {
		// no newline found, just return from position
		strTo = to
	} else {
		// new line found, need to decrement position to omit newline byte
		strTo--
	}
	return strFrom, strTo, nil
}

// getString returns string from `file` in [from; to]
func getString(file *os.File, from int64, to int64) (string, error) {
	bufferSize := to - from + 1
	buffer := make([]byte, bufferSize)

	_, err := file.Seek(from, 0)
	if err != nil {
		return "", err
	}

	_, err = file.Read(buffer)
	if err != nil {
		return "", err
	}

	return string(buffer[:bufferSize]), nil
}

func (c *Component) seekFile() {
	_, err := c.file.ptr.Seek(100, 0)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(c.file.ptr)
	for scanner.Scan() {

		fmt.Println(scanner.Text())
	}
}
