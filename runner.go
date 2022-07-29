package main

import (
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
)

// Absolute paths to drivers.
const (
	scyllaGoPath   = ""
	gocqlPath      = ""
	scyllaRustPath = ""
)

// For testing if all drivers are setup correctly.
// var (
// 	addr        = "192.168.100.100:9042"
// 	runs        = 1
// 	workloads   = []string{"mixed"}
// 	tasks       = []int{1_000_000}
// 	concurrency = []int{1024}
// 	cpu         = runtime.NumCPU()
// )

var (
	addr        = "192.168.100.100:9042"
	runs        = 5
	workloads   = []string{"inserts", "mixed"}
	tasks       = []int{1_000_000, 10_000_000, 100_000_000}
	concurrency = []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}
	cpu         = runtime.NumCPU()
)

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

func runBenchmark(name, cmd, path string) []benchResult {
	var results []benchResult
	for _, workload := range workloads {
		for _, tasksNum := range tasks {
			for _, concurrencyNum := range concurrency {
				result := newBenchResult(name, workload, runs, tasksNum, concurrencyNum)
				cmdWithFlags := addFlags(cmd, workload, addr, tasksNum, concurrencyNum)
				for i := 0; i < runs; i++ {
					time.Sleep(time.Second)
					log.Printf("%s - run: %v, workload: %s, tasks: %v, concurrency: %v", name, i+1, workload, tasksNum, concurrencyNum)
					log.Println(cmdWithFlags)
					out, err := exec.Command("/bin/sh", "-c", "cd "+path+"; "+cmdWithFlags+";").CombinedOutput()
					if err != nil {
						panic(fmt.Errorf("%w log:\n%s", err, out))
					}
					t := getTime(string(out))
					log.Printf(" time: %v\n", t)
					result.insert(t, i)
				}
				result.calculateMeanAndDev()
				results = append(results, result)
			}
		}
	}

	return results
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
	scyllaRustResults := runBenchmark("scylla-rust-driver", "cargo run --release .", scyllaRustPath)
	gocqlResults := runBenchmark("gocql", "go run .", gocqlPath)
	scyllaGoResults := runBenchmark("scylla-go-driver", "go run .", scyllaGoPath)

	var results []benchResult
	results = append(results, scyllaGoResults...)
	results = append(results, scyllaRustResults...)
	results = append(results, gocqlResults...)

	makeCSV("./benchmarkResults", results)
}
