package search

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gotomicro/ego/core/elog"

	"github.com/clickvisual/clickvisual/api/internal/pkg/cvdocker"
	view2 "github.com/clickvisual/clickvisual/api/internal/pkg/model/view"
	"github.com/clickvisual/clickvisual/api/internal/pkg/utils/gopool"
)

type Container struct {
	components []*Component
}

// Component 每个执行指令地方
type Component struct {
	request     Request
	file        *File
	startTime   int64
	endTime     int64
	words       []KeySearch
	filterWords []string // 变成匹配的语句
	bash        *Bash
	limit       int64
	output      []string
	logs        []map[string]interface{}
	charts      map[int64]int64 // key: offset, value: count
	maxTimes    int64           // 记录该文件下最大的时间 offset
	interval    int64
}

type KeySearch struct {
	Key   string
	Value string
	Type  string
}

type CmdRequest struct {
	StartTime    string
	EndTime      string
	Date         string // last 30min,6h,1d,7d
	Path         string // 文件路径
	Dir          string // 文件夹路径
	KeyWord      string // 搜索的关键词
	Limit        int64  // 最少多少条数据
	IsK8S        bool
	K8SContainer []string
}

func (req *Request) IsChartsRequest() bool {
	return req.Interval > 0
}

func (c *Component) IsChartsRequest() bool {
	return c.interval > 0
}

func (c CmdRequest) ToRequest() Request {
	var (
		st int64
		et int64
	)

	if c.StartTime != "" {
		sDate, err := time.ParseInLocation(time.DateTime, c.StartTime, time.Local)
		st = sDate.Unix()
		if err != nil {
			elog.Panic("parse start time error", elog.FieldErr(err))
		}
	}

	if c.EndTime != "" {
		eDate, err := time.ParseInLocation(time.DateTime, c.EndTime, time.Local)
		et = eDate.Unix()
		if err != nil {
			elog.Panic("parse end time error", elog.FieldErr(err))
		}
	}

	return Request{
		StartTime:    st,
		EndTime:      et,
		Date:         c.Date,
		Path:         c.Path,
		Dir:          c.Dir,
		KeyWord:      c.KeyWord,
		Limit:        c.Limit,
		IsCommand:    true,
		IsK8S:        c.IsK8S,
		K8SContainer: c.K8SContainer,
	}
}

type Request struct {
	StartTime     int64
	EndTime       int64
	Date          string // last 30min,6h,1d,7d
	Path          string // 文件路径
	Dir           string // 文件夹路径
	TruePath      []string
	KeyWord       string // 搜索的关键词
	Limit         int64  // 最少多少条数据
	Interval      int64  // calc 后的标准时间间隔
	IsCommand     bool   // 是否是命令行 默认不是
	IsK8S         bool
	K8SContainer  []string
	K8sClientType string // 是 containerd，还是docker
}
type FileSearchResp struct {
	Logs   []map[string]interface{}
	Charts []*view2.HighChart
}

func Run(req Request) (resp FileSearchResp, err error) {
	var filePaths []string
	// 如果filename为空字符串，分割会得到一个长度为1的空字符串数组
	// todo 需要做优化，不用重复new
	if req.IsK8S {
		obj := cvdocker.NewContainer()
		req.K8sClientType = obj.ClientType
		containers := obj.GetActiveContainers()
		for _, value := range containers {
			if len(req.K8SContainer) == 0 {
				filePaths = append(filePaths, value.LogPath)
			} else {
				for _, v := range req.K8SContainer {
					if value.K8SInfo.Container == v {
						filePaths = append(filePaths, value.LogPath)
					}
				}
			}

		}
	}
	if req.Path != "" {
		filePaths = strings.Split(req.Path, ",")
	}
	if req.Dir != "" {
		filePathsByDir := findFiles(req.Dir)
		filePaths = append(filePaths, filePathsByDir...)
	}
	req.TruePath = filePaths
	if len(filePaths) == 0 {
		panic("file cant empty")
	}
	// 多了没意义，自动变为 50，提示用户
	if !req.IsChartsRequest() && (req.Limit <= 0 || req.Limit > 500) {
		req.Limit = 50
		elog.Info("limit exceeds 500. it will be automatically set to 50", elog.Int64("limit", req.Limit))
	}
	elog.Info("agent log search start", elog.Any("req", req))
	container := &Container{}
	l := sync.WaitGroup{}
	// 文件添加并发查找
	l.Add(len(filePaths))
	// fmt.Println("files len: ", len(filePaths))
	// now := time.Now()
	for _, pathName := range filePaths {
		value := pathName
		go func() {
			comp := NewComponent(
				value,
				req,
			)
			if req.KeyWord != "" && len(comp.words) == 0 {
				elog.Error("-k format is error", elog.FieldErr(err))
				l.Done()
				return
			}
			container.components = append(container.components, comp)
			comp.SearchFile()
			comp.file.ptr.Close()
			l.Done()
		}()
	}
	l.Wait()
	// delta := time.Since(now)
	// fmt.Println("Agent Goroutine Search File Time: ", delta)

	// true if this request for logs
	if !req.IsChartsRequest() {
		if req.IsCommand {
			for _, comp := range container.components {
				fmt.Println(comp.bash.ColorAll(comp.file.path))
				for _, value := range comp.output {
					fmt.Println(value)
				}
			}
		} else {
			resp.Logs = make([]map[string]interface{}, 0)
			for _, comp := range container.components {
				resp.Logs = append(resp.Logs, comp.logs...)
			}
		}
		return
	}

	resp.Charts = make([]*view2.HighChart, 0)
	times := (req.EndTime - req.StartTime) / req.Interval
	var i int64 = 0

	wrote := false
	maxTimes := int64(0)
	for i < times {
		var total int64 = 0
		for j, comp := range container.components {
			if j == 0 {
				if comp.maxTimes > maxTimes {
					maxTimes = comp.maxTimes
				}
			}
			if count, ok := comp.charts[i]; ok {
				total += count
			}
		}

		// API layer will scan data and add head 、tail zero count interval, so we need to adapt
		// filter head and tail zero count interval
		if (wrote && i < maxTimes) || total != 0 {
			wrote = true
			endTime := req.StartTime + (i+1)*req.Interval
			if endTime > req.EndTime {
				endTime = req.EndTime
			}
			resp.Charts = append(resp.Charts, &view2.HighChart{
				Count: uint64(total),
				From:  req.StartTime + i*req.Interval,
				To:    endTime,
			})
			// fmt.Printf("i: %d, startTime: %d, endTime: %d, interval: %d\n", req.StartTime+i*req.Interval, endTime, req.Interval, i)
		}
		i++
	}
	return
}

func NewComponent(filename string, req Request) *Component {
	obj := &Component{}
	file, err := OpenFile(filename)
	if err != nil {
		elog.Error("agent open log file error", elog.FieldErr(err), elog.String("path", filename))
	}
	// request for charts
	if req.IsChartsRequest() {
		obj.charts = make(map[int64]int64)
	}
	obj.interval = req.Interval
	obj.file = file
	obj.startTime = req.StartTime
	obj.endTime = req.EndTime
	obj.request = req
	words := make([]KeySearch, 0)

	arrs := strings.Split(req.KeyWord, "and")
	for _, value := range arrs {
		if strings.Contains(value, "=") {
			info := strings.Split(value, "=")
			v := strings.Trim(info[1], " ")
			v = strings.ReplaceAll(v, `"`, "")
			v = strings.ReplaceAll(v, `'`, "")
			word := KeySearch{
				Key:   strings.Trim(info[0], " "),
				Value: v,
			}
			words = append(words, word)
		}
	}
	obj.words = words
	filterString := make([]string, 0)
	for _, value := range words {
		var info string
		if value.Type == "int" {
			info = `"` + value.Key + `":` + value.Value
		} else {
			info = `"` + value.Key + `":"` + value.Value + `"`
		}
		filterString = append(filterString, info)
	}
	obj.filterWords = filterString
	obj.bash = NewBash()
	obj.limit = req.Limit
	return obj
}

/*
 * searchFile 搜索文件内容
 * searchFile 2023-09-28 10:10:00 2023-09-28 10:20:00 /xxx/your_service.log`
 */
func (c *Component) SearchFile() ([]map[string]interface{}, error) {
	defer c.file.ptr.Close()
	if c.file.size == 0 {
		panic("file size is 0")
	}
	var (
		start = int64(0)
		end   = c.file.size
		err   error
	)
	if c.IsChartsRequest() && len(c.filterWords) == 0 {
		// now := time.Now()
		times := (c.endTime - c.startTime) / c.interval
		// times = 1
		originStartTime, originEndTime := c.startTime, c.endTime
		var i int64
		var endTime int64
		st := c.startTime
		// fmt.Printf("########## Start: %d, End: %d\n", start, end)
		for i = 1; i <= times; i++ {
			endTime = st + i*c.interval
			if endTime > originEndTime {
				endTime = originEndTime
			}
			c.endTime = endTime
			// searhEndTime := time.Now()
			end, err = c.searchByEndTime()
			// delat := time.Since(searhEndTime)
			// fmt.Printf("Agent SearchByEndTime cnt: %d, file: %s, Use Time: %d\n", i, c.file.path, delat.Milliseconds())
			if err != nil {
				elog.Error("agent search ts error", elog.FieldErr(err))
				panic("agent search timestamp error")
			}
			// if true, after this turn, endTime will be larger, end always be -1
			if end == -1 {
				continue
			}

			// calcTime := time.Now()
			// fmt.Printf("########## Start: %d, End: %d\n", start, end)
			count := c.calcLinesGoroutine(start, end)
			// delat := time.Since(calcTime)
			// fmt.Printf("Agent calcLines cnt: %d, file: %s, Use Time: %d\n", i, c.file.path, delat.Milliseconds())
			c.charts[i-1] = count
			c.maxTimes = i - 1
			start = end + 2
		}
		c.startTime, c.endTime = originStartTime, originEndTime

		// delta := time.Since(now)
		// fmt.Printf("Agent Aggregate logs, file: %s, Use Time: %d\n", c.file.path, delta.Milliseconds())
		return nil, nil
	}

	if c.startTime > 0 {
		start, err = c.searchByStartTime()
		if err != nil {
			elog.Error("agent search ts error", elog.FieldErr(err))
		}
	}
	if c.endTime > 0 {
		end, err = c.searchByEndTime()
		if err != nil {
			elog.Error("agent search ts error", elog.FieldErr(err))
		}
	}

	if start != -1 && start <= end {
		if c.IsChartsRequest() {
			c.searchChartsByBackWord(start, end)
			return nil, nil
		} else {
			_, err = c.searchByBackWord(start, end)
			if err != nil {
				elog.Error("searchByBackWord error", elog.FieldErr(err))
				return nil, nil
			}
		}
		if len(c.logs) == 0 {
			elog.Info("agent log search nothing", elog.Any("words", c.words))
		}
		return c.logs, err
	}
	elog.Info("agent log search nothing", elog.Any("words", c.words))
	return nil, nil
}

func calc(startTime, endTime, interval int64, path string) (offset, count int64) {
	file, err := OpenFile(path)
	if err != nil {
		panic(err)
	}
	offset = startTime / interval

	cc := Component{
		startTime: startTime,
		endTime:   endTime,
		file:      file,
	}
	startPos, _ := cc.searchByStartTime()
	endPos, _ := cc.searchByEndTime()
	file.ptr.Seek(startPos, 0)
	scanner := bufio.NewScanner(file.ptr)
	count = 0
	i := 0
	for scanner.Scan() {
		if endPos < int64(i) {
			file.ptr.Close()
			break
		}
		i += len(scanner.Text())
		count++
		fmt.Printf("### scan count %d, time: %d\n", offset, startTime)
	}
	return
}

func (c *Component) getCharts() {
	n := int((c.endTime - c.startTime) / c.interval)
	intervals := make([]int64, n)
	intervals[0] = c.startTime
	for i := 1; i < n; i++ {
		intervals[i] = intervals[i-1] + c.interval
	}

	if intervals[n-1] > c.endTime {
		intervals[n-1] = c.endTime
	}

	pool := gopool.NewPool("log-charts", int32(2), gopool.NewConfig())

	var wg sync.WaitGroup

	channel := make(chan [2]int64, n)
	ctx, cancle := context.WithCancel(context.Background())
	wg.Add(n)
	for i := 0; i < n-1; i++ {
		pool.Go(func() {
			offset, count := calc(intervals[i], intervals[i+1], c.interval, c.file.path)
			fmt.Printf("offset: %d, count: %d\n", offset, count)
			channel <- [2]int64{offset, count}
			wg.Done()
		})
		fmt.Printf("submit %d 任务～\n", i)
	}
	fmt.Println("poolSize: ", pool.WorkerCount())

	go func() {
		for {
			select {
			case ch := <-channel:
				c.charts[ch[0]] = ch[1]
			case <-ctx.Done():
				close(channel)
				return
			}
		}
	}()

	fmt.Println("wait for pool run1")
	wg.Wait()
	fmt.Println("wait for pool run over")
	cancle()
	fmt.Println("wait for pool run orver -2")
}
