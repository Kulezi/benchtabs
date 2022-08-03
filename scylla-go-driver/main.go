package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mmatczuk/scylla-go-driver"
	"github.com/pkg/profile"
)

const insertStmt = "INSERT INTO benchtab (pk, v1, v2) VALUES(?, ?, ?)"
const selectStmt = "SELECT v1, v2 FROM benchtab WHERE pk = ?"

const samples = 20_000

func main() {
	config := readConfig()
	log.Printf("Config %#+v", config)

	if config.profileCPU && config.profileMem {
		log.Fatal("select one profile type")
	}
	if config.profileCPU {
		log.Println("Running with CPU profiling")
		defer profile.Start(profile.CPUProfile).Stop()
	}
	if config.profileMem {
		log.Println("Running with memory profiling")
		defer profile.Start(profile.MemProfile).Stop()
	}

	cfg := scylla.DefaultSessionConfig("", config.nodeAddresses...)
	cfg.Username = config.user
	cfg.Password = config.password
	cfg.Timeout = 30 * time.Second

	if !config.dontPrepare {
		initSession, err := scylla.NewSession(cfg)
		if err != nil {
			log.Fatal(err)
		}
		initKeyspaceAndTable(initSession, config.keyspace)
		initSession.Close()
	}

	cfg.Keyspace = config.keyspace
	session, err := scylla.NewSession(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if config.workload == Selects && !config.dontPrepare {
		initSelectsBenchmark(session, config)
	}

	benchmark(&config, session)

}

// benchmark is the same as in gocql.
func benchmark(config *Config, session *scylla.Session) {
	var wg sync.WaitGroup
	nextBatchStart := -config.batchSize

	log.Println("Starting the benchmark")
	startTime := time.Now()

	selectCh := make(chan time.Duration, 2*samples)
	insertCh := make(chan time.Duration, 2*samples)
	for i := int64(0); i < config.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertQ, err := session.Prepare(insertStmt)
			if err != nil {
				log.Fatal(err)
			}
			selectQ, err := session.Prepare(selectStmt)
			if err != nil {
				log.Fatal(err)
			}

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					sample := false
					var startTime time.Time
					if rand.Int63n(config.tasks) < samples {
						sample = true
					}

					if config.workload == Inserts || config.workload == Mixed {
						if sample {
							startTime = time.Now()
						}
						_, err := insertQ.BindInt64(0, pk).BindInt64(1, 2*pk).BindInt64(2, 3*pk).Exec()
						if err != nil {
							panic(err)
						}
						if sample {
							insertCh <- time.Now().Sub(startTime)
						}
					}

					if config.workload == Selects || config.workload == Mixed {
						if sample {
							startTime = time.Now()
						}

						var v1, v2 int64
						res, err := selectQ.BindInt64(0, pk).Exec()
						if err != nil {
							panic(err)
						}

						v1, err = res.Rows[0][0].AsInt64()
						if err != nil {
							log.Fatal(err)
						}
						v2, err = res.Rows[0][1].AsInt64()
						if err != nil {
							log.Fatal(err)
						}
						if v1 != 2*pk || v2 != 3*pk {
							log.Fatalf("expected (%d, %d), got (%d, %d)", 2*pk, 3*pk, v1, v2)
						}

						if sample {
							selectCh <- time.Now().Sub(startTime)
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	benchTime := time.Now().Sub(startTime)
	log.Printf("Finished\nBenchmark time: %d ms\n", benchTime.Milliseconds())
	fmt.Printf("time %d\n", benchTime.Milliseconds())
	printLatencyInfo("select", selectCh)
	printLatencyInfo("insert", insertCh)
}

func printLatencyInfo(name string, ch chan time.Duration) {
	cnt := len(ch)
	for i := 0; i < cnt; i++ {
		fmt.Printf("%s %d\n", name, (<-ch).Nanoseconds())
	}
}

func initKeyspaceAndTable(session *scylla.Session, ks string) {
	q := session.Query("DROP KEYSPACE IF EXISTS " + ks)
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	q = session.Query("CREATE KEYSPACE " + ks + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	q = session.Query("CREATE TABLE " + ks + ".benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)")
	if _, err := q.Exec(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
}

func initSelectsBenchmark(session *scylla.Session, config Config) {
	log.Println("inserting values...")

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	for i := int64(0); i < max(1024, config.workers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			insertQ, err := session.Prepare(insertStmt)
			if err != nil {
				log.Fatal(err)
			}

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					insertQ.BindInt64(0, pk)
					insertQ.BindInt64(1, 2*pk)
					insertQ.BindInt64(2, 3*pk)
					if _, err := insertQ.Exec(); err != nil {
						log.Fatal(err)
					}
				}
			}
		}()
	}

	wg.Wait()
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
