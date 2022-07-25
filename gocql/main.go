package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const insertStmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)"
const selectStmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?"

func main() {
	config := readConfig()
	fmt.Printf("Benchmark configuration: %#v\n", config)

	cluster := gocql.NewCluster(config.nodeAddresses[:]...)
	cluster.Timeout = 5 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	if !config.dontPrepare {
		prepareKeyspaceAndTable(session)
	}

	if config.workload == Selects && !config.dontPrepare {
		prepareSelectsBenchmark(session, config)
	}

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	fmt.Println("Starting the benchmark")

	startTime := time.Now()

	for i := int64(0); i < config.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertQ := session.Query(insertStmt)
			selectQ := session.Query(selectStmt)

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					if config.workload == Inserts || config.workload == Mixed {
						err := insertQ.Bind(pk, 2*pk, 3*pk).Exec()
						if err != nil {
							panic(err)
						}
					}

					if config.workload == Selects || config.workload == Mixed {
						var v1, v2 int64

						err := selectQ.Bind(pk).Scan(&v1, &v2)
						if err != nil {
							panic(err)
						}

						if v1 != 2*pk || v2 != 3*pk {
							panic("bad data")
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	benchTime := time.Now().Sub(startTime)

	fmt.Printf("Finished\nBenchmark time: %d ms\n", benchTime.Milliseconds())
}

func awaitSchemaAgreement(session *gocql.Session) {
	err := session.AwaitSchemaAgreement(context.Background())
	if err != nil {
		panic(err)
	}
}

func prepareKeyspaceAndTable(session *gocql.Session) {
	err := session.Query("DROP KEYSPACE IF EXISTS benchks").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)

	err = session.Query("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)

	err = session.Query("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)").Exec()
	if err != nil {
		panic(err)
	}
	awaitSchemaAgreement(session)
}

func prepareSelectsBenchmark(session *gocql.Session, config Config) {
	fmt.Println("Preparing a selects benchmark (inserting values)...")

	var wg sync.WaitGroup
	nextBatchStart := int64(0)

	for i := int64(0); i < max(1024, config.concurrency); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			insertQ := session.Query(insertStmt)

			for {
				curBatchStart := atomic.AddInt64(&nextBatchStart, config.batchSize)
				if curBatchStart >= config.tasks {
					// no more work to do
					break
				}

				curBatchEnd := min(curBatchStart+config.batchSize, config.tasks)

				for pk := curBatchStart; pk < curBatchEnd; pk++ {
					err := insertQ.Bind(pk, 2*pk, 3*pk).Exec()
					if err != nil {
						panic(err)
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
