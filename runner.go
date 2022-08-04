package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/jamiealquiza/tachymeter"
)

// Absolute paths to drivers.
const (
	benchPath      = "/home/ubuntu/benchtabs"
	scyllaGoPath   = "scylla-go-driver"
	gocqlPath      = "gocql"
	scyllaRustPath = "scylla-rust-driver/src"
	cppPath        = "cpp"

	outPath = benchPath + "/results/"
	logPath = benchPath + "/running.log"

	nap = 5
)

// For testing if all drivers are setup correctly.
var (
	addr        = "192.168.100.100"
	runs        = 1
	workloads   = []string{"mixed"}
	tasks       = []int{1_000_000}
	concurrency = []int{1024}
	cpu         = runtime.NumCPU()
)

// var (
// 	addr        = "192.168.100.100:9042"
// 	runs        = 5
// 	workloads   = []string{"inserts", "mixed"}
// 	tasks       = []int{1_000_000, 10_000_000, 100_000_000}
// 	concurrency = []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}
// 	cpu         = runtime.NumCPU()
// )

type benchResult struct {
	name        string
	workload    string
	tasks       int
	concurrency int
	time        []int
	mean        float64
	dev         float64
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func newBenchResult(name, workload string, runs, tasks, concurrency int) benchResult {
	return benchResult{
		name:        name,
		workload:    workload,
		tasks:       tasks,
		concurrency: concurrency,
		time:        make([]int, runs),
	}
}

func (r *benchResult) insert(t, pos int) {
	r.time[pos] = t
}

func (r *benchResult) calculateMeanAndDev() {
	sum := 0
	for _, t := range r.time {
		sum += t
	}
	r.mean = float64(sum / len(r.time))

	sq := float64(0)
	for _, t := range r.time {
		sq += (float64(t) - r.mean) * (float64(t) - r.mean)
	}

	r.dev = math.Sqrt((sq / float64(len(r.time))))
}

func getTime(input string) int {
	reg := regexp.MustCompile("Benchmark time: ([0-9]+) ms") // nolint:gocritic
	t, err := strconv.Atoi(reg.FindStringSubmatch(input)[1])
	if err != nil {
		panic(err)
	}

	return t
}

func addFlags(cmd, workload, addr string, tasks, concurrency int) string {
	return cmd + " --nodes " + addr + " --workload " + workload + " --tasks " + strconv.Itoa(tasks) + " --concurrency " + strconv.Itoa(concurrency)
}

func runBenchmark(name, cmd, path string) {
	for _, workload := range workloads {
		for _, tasksNum := range tasks {
			for _, concurrencyNum := range concurrency {
				cmdWithFlags := addFlags(cmd, workload, addr, tasksNum, concurrencyNum)
				for i := 0; i < runs; i++ {
					runOut := fmt.Sprintf("%s%s_workload=%s_tasks=%d_concurrency=%d_run=%d", outPath, name, workload, tasksNum, concurrencyNum, i+1)
					log.Printf("%s - workload: %s, tasks: %v, concurrency: %v, run: %v, ", name, workload, tasksNum, concurrencyNum, i+1)
					log.Println(cmdWithFlags)
					out, err := exec.Command(
						"/bin/sh",
						"-c",
						"cd "+path+"; /usr/bin/time "+cmdWithFlags+" >"+runOut+" 2>>"+logPath+";").CombinedOutput()
					if err != nil {
						panic(fmt.Errorf("%w output:\n%s", err, out))
					}

					fmt.Printf("%s, %s, %v, %v, %v, ", name, workload, tasksNum, concurrencyNum, i+1)
					printParsedResultsFromFile(runOut, tasksNum)

					time.Sleep(nap * time.Second)
				}

			}
		}
	}

}

func printParsedResultsFromFile(path string, tasksNum int) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	r := bufio.NewReader(f)

	var selects, inserts []time.Duration
	for {
		var typ string
		var t int64
		_, err := fmt.Fscan(r, &typ)
		if err != nil {
			break
		}

		switch typ {
		case "time":
			fmt.Fscan(r, &t)
			log.Printf("benchmark time: %dms\n", t)
			fmt.Printf("%dms", t)
		case "select":
			fmt.Fscan(r, &t)
			selects = append(selects, time.Duration(t))
		case "insert":
			fmt.Fscan(r, &t)
			inserts = append(inserts, time.Duration(t))
		default:
			continue
		}
	}

	printLatencyInfo("select", selects, tasksNum)
	printLatencyInfo("insert", inserts, tasksNum)
	fmt.Println()
}

func printLatencyInfo(name string, samples []time.Duration, tasksNum int) {
	t := tachymeter.New(&tachymeter.Config{Size: tasksNum})

	for _, v := range samples {
		t.AddTime(v)
	}

	metrics := t.Calc()
	log.Println(metrics)
	fmt.Printf(", %dns, %dns, %dns", metrics.Time.Avg.Nanoseconds(), metrics.Time.StdDev.Nanoseconds(), metrics.Time.P99.Nanoseconds())
}

func makeCSV(out string, results []benchResult) {
	csvFile, err := os.Create(out + ".csv")
	if err != nil {
		panic(csvFile)
	}
	csvWriter := csv.NewWriter(csvFile)

	head := []string{"Driver", "Workload", "Tasks", "concurrency", "Time", "Standard Deviation"}
	err = csvWriter.Write(head)
	if err != nil {
		panic(err)
	}

	for _, result := range results {
		row := []string{
			result.name,
			result.workload,
			strconv.Itoa(result.tasks),
			strconv.Itoa(result.concurrency),
			fmt.Sprintf("%f", result.mean),
			fmt.Sprintf("%f", result.dev),
		}

		err = csvWriter.Write(row)
		if err != nil {
			panic(err)
		}
	}

	csvWriter.Flush()
	err = csvFile.Close()
	if err != nil {
		panic(err)
	}
}

func main() {
	fmt.Println("driver, workload, tasks, concurrency, run, bench_time, select_avg, select_stddev, select_p99, insert_avg, insert_stddev, insert_p99")

	runBenchmark("cpp", "./benchmark", cppPath)
	runBenchmark("scylla-rust-driver", "cargo run --release .", scyllaRustPath)
	runBenchmark("gocql", "go run .", gocqlPath)
	runBenchmark("scylla-go-driver", "go run .", scyllaGoPath)
}
