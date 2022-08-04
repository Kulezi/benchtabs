#include <atomic>
#include <cassandra.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <utility>
#include <vector>
#include <thread>
#include <random>

#include "config.hpp"
#include "semaphore.hpp"

const int SAMPLES = 20000;

void assert_future_ok(CassFuture *future, const char *message) {
    if (cass_future_error_code(future) == CASS_OK) {
        return;
    }

    const char *error_msg;
    size_t error_msg_len;
    cass_future_error_message(future, &error_msg, &error_msg_len);
    fprintf(stderr, "ERROR: %s: '%.*s'\n", message, (int)error_msg_len, error_msg);

    std::exit(1);
}

std::pair<CassSession*, CassCluster*> connect(Config& config) {
    CassCluster *cluster = cass_cluster_new();
    if (cass_cluster_set_queue_size_io(cluster, std::max((int64_t)2048, 2 * config.concurrency)) != CASS_OK) {
        fprintf(stderr, "ERROR: Failed to set io queue size\n");
        std::exit(1);
    }

    if (cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4) != CASS_OK) {
        fprintf(stderr, "ERROR: Failed to set protocol version\n");
        std::exit(1);
    }

    for (auto&& node_address : config.node_addresses) {
        cass_cluster_set_contact_points(cluster, node_address.c_str());
    }

    if (cass_cluster_set_num_threads_io(cluster, std::thread::hardware_concurrency()) != CASS_OK) {
       fprintf(stderr, "ERROR: Failed to set io threads number");
       std::exit(1);
    }

    CassSession *session = cass_session_new();
    CassFuture *connect_future = cass_session_connect(session, cluster);
    cass_future_wait(connect_future);

    assert_future_ok(connect_future, "Unable to connect");
    cass_future_free(connect_future);

    // Cluster gets leaked, it's ok this is just a benchmark
    return std::make_pair(session, cluster);
}

void run_simple_query(CassSession *session, const char *query) {
    CassStatement *statement = cass_statement_new(query, 0);

    CassFuture *result_future = cass_session_execute(session, statement);
    cass_future_wait(result_future);

    assert_future_ok(result_future, "Simple query failed");

    cass_future_free(result_future);
    cass_statement_free(statement);
}

const CassPrepared *prepare_query(CassSession* session, const char *query) {
    CassFuture *prepare_future = cass_session_prepare(session, query);
    cass_future_wait(prepare_future);
    assert_future_ok(prepare_future, "Preparing failed");

    const CassPrepared *result = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    return result;
}

struct CallbackData {
    Config *config;
    CassSession *session;
    CassStatement *insert_stmt;
    CassStatement *select_stmt;

    std::atomic<int64_t> *next_batch_start;
    int64_t cur_pk;
    int64_t end_pk;

    std::vector<int64_t> selects;
    std::vector<int64_t> inserts;
    std::mt19937 rng;

    bool sample;
    int64_t sample_start;

    Semaphore *finished_semaphore;
};

int64_t get_time_in_nanos() {
    auto time = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
}

void run_concurrent_task(CallbackData *);
void select_callback(CassFuture *, void *);

void insert_callback(CassFuture *insert_future, void *data) {
    CallbackData *cb_data = (CallbackData *)data;

    assert_future_ok(insert_future, "Insert failed");
    cass_future_free(insert_future);

    // Stop timing insert.
    if (cb_data->sample) {
        cb_data->inserts.push_back(get_time_in_nanos() - cb_data->sample_start);
    }

    if (cb_data->config->workload == Workload::Inserts) {
        // Ok all done, start next insert
        cb_data->cur_pk += 1;
        run_concurrent_task(cb_data);
        return;
    }

    // In case of Workload::Mixed we need to perform a select after insert
    // Start timing select.
    if (cb_data->sample) {
        cb_data->sample_start = get_time_in_nanos();
    }

    cass_statement_bind_int64(cb_data->select_stmt, 0, cb_data->cur_pk);

    CassFuture *select_future = cass_session_execute(cb_data->session, cb_data->select_stmt);
    cass_future_set_callback(select_future, select_callback, (void *)cb_data);
}

void select_callback(CassFuture *select_future, void *data) {
    CallbackData *cb_data = (CallbackData *)data;

    assert_future_ok(select_future, "Select failed");

    // Ensure that returned row contains 2*pk and 3*pk
    const CassResult *result = cass_future_get_result(select_future);
    CassIterator *res_iter = cass_iterator_from_result(result);

    if (!cass_iterator_next(res_iter)) {
        fprintf(stderr, "ERROR: Select did not return any rows!\n");
        std::exit(1);
    }

    const CassRow *row = cass_iterator_get_row(res_iter);

    int64_t pk = cb_data->cur_pk;
    int64_t v1 = 0;
    int64_t v2 = 0;

    cass_value_get_int64(cass_row_get_column(row, 0), &v1);
    cass_value_get_int64(cass_row_get_column(row, 1), &v2);

    if (v1 != 2 * cb_data->cur_pk || v2 != 3 * cb_data->cur_pk) {
        fprintf(stderr, "ERROR: Invalid row: (pk, v1, v2) = (%ld, %ld, %ld)\n", pk, v1, v2);
        std::exit(1);
    }

    cass_iterator_free(res_iter);
    cass_result_free(result);
    cass_future_free(select_future);

    // Stop timing select.
    if (cb_data->sample) {
        cb_data->selects.push_back(get_time_in_nanos() - cb_data->sample_start);
    }

    // Continue running concurrent task
    cb_data->cur_pk += 1;
    run_concurrent_task(cb_data);
}

void run_concurrent_task(CallbackData *cb_data) {
    Config* config = cb_data->config;

    cb_data->sample = std::uniform_int_distribution<int>(0, config->tasks)(cb_data->rng) < SAMPLES;
    // If current batch has finished we need to acquire a new one
    if (cb_data->cur_pk >= cb_data->end_pk) {
        int64_t new_batch_start = cb_data->next_batch_start->fetch_add(config->batch_size);

        if (new_batch_start >= config->tasks) {
            // No more work to do
            cb_data->finished_semaphore->free_permit();
            return;
        }

        cb_data->cur_pk = new_batch_start;
        cb_data->end_pk = std::min(new_batch_start + config->batch_size, config->tasks);
    }

    if (config->workload == Workload::Inserts || config->workload == Workload::Mixed) {
        // Start timing insert.
        if (cb_data->sample) {
            cb_data->sample_start = get_time_in_nanos();
        }

        // Perform an insert
        cass_statement_bind_int64(cb_data->insert_stmt, 0, cb_data->cur_pk);
        cass_statement_bind_int64(cb_data->insert_stmt, 1, 2 * cb_data->cur_pk);
        cass_statement_bind_int64(cb_data->insert_stmt, 2, 3 * cb_data->cur_pk);

        CassFuture *insert_future = cass_session_execute(cb_data->session, cb_data->insert_stmt);
        cass_future_set_callback(insert_future, insert_callback, (void *)cb_data);
    }

    if (config->workload == Workload::Selects) {
        // Start timing select.
        if (cb_data->sample) {
            cb_data->sample_start = get_time_in_nanos();
        }

        // Perform a select
        cass_statement_bind_int64(cb_data->select_stmt, 0, cb_data->cur_pk);

        CassFuture *select_future = cass_session_execute(cb_data->session, cb_data->select_stmt);
        cass_future_set_callback(select_future, select_callback, (void *)cb_data);
    }
}

void prepare_keyspace_and_table(CassSession*);
void prepare_selects_benchmark(CassSession*, const CassPrepared*, Config&);

int main(int argc, const char *argv[]) {
    std::cerr << "Starting cpp-driver benchmark!\n\n";

    // Parse config
    Config config(argc, argv);

    std::cerr << "Benchmark configuration:\n";
    config.print();

    // Connect to the cluster
    auto [session, cluster] = connect(config);

    // Prepare before the benchmark
    if (!config.dont_prepare) {
        prepare_keyspace_and_table(session);
    }

    const CassPrepared* prepared_insert = prepare_query(session, "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)");
    const CassPrepared* prepared_select = prepare_query(session, "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?");

    if (config.workload == Workload::Selects && !config.dont_prepare) {
        prepare_selects_benchmark(session, prepared_insert, config);
    }

    // Start concurrent tasks
    std::vector<CallbackData> callbacks_data(config.concurrency);
    Semaphore finished_semaphore(config.concurrency);
    std::atomic<int64_t> next_batch_start(0);

    std::cerr << "\nStarting the benchmark\n";
    auto start_time = std::chrono::high_resolution_clock::now();

    std::mt19937 rng(start_time.time_since_epoch().count());
    for (int64_t c = 0; c < config.concurrency; c++) {
        callbacks_data[c] = CallbackData {
            .config = &config,
            .session = session,
            .insert_stmt = cass_prepared_bind(prepared_insert),
            .select_stmt = cass_prepared_bind(prepared_select),

            .next_batch_start = &next_batch_start,
            .cur_pk = 0,
            .end_pk = 0,

            .selects = {},
            .inserts = {},
            .rng = std::mt19937(rng()),
            .finished_semaphore = &finished_semaphore
        };

        finished_semaphore.acquire_permit();
        run_concurrent_task(&callbacks_data[c]);
    }

    // Wait for concurrent tasks to finish
    for (int64_t c = 0; c < config.concurrency; c++) {
        finished_semaphore.acquire_permit();
    }

    for (auto&& cb_data : callbacks_data) {
        cass_statement_free(cb_data.insert_stmt);
        cass_statement_free(cb_data.select_stmt);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cerr << "Finished\n\nBenchmark time: " << millis.count() << " ms\n";
    std::cout << "time " << millis.count() << "\n";
    for (int64_t c = 0; c < config.concurrency; c++) {
        for (auto &i: callbacks_data[c].selects) {
            std::cout << "select " << i << "\n";
        }
        for (auto &i: callbacks_data[c].inserts) {
            std::cout << "insert " << i << "\n";
        }
    }

    cass_prepared_free(prepared_insert);
    cass_prepared_free(prepared_select);

    cass_session_free(session);
    cass_cluster_free(cluster);
    return 0;
}

void prepare_keyspace_and_table(CassSession* session) {
    run_simple_query(session, "DROP KEYSPACE IF EXISTS benchks");

    run_simple_query(session, "CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' "
                        ": 'SimpleStrategy', 'replication_factor' : 1}");

    run_simple_query(session, "CREATE TABLE IF NOT EXISTS benchks.benchtab (pk "
                        "bigint PRIMARY KEY, v1 bigint, v2 bigint)");
}

void prepare_selects_benchmark(CassSession *session, const CassPrepared* prepared_insert, Config& config) {
    // Before performing selects we have to insert values to select
    std::cerr << "Preparing a selects benchmark (inserting values)...\n";

    // Pretend the workload is Inserts and run concurrent tasks
    config.workload = Workload::Inserts;

    // Use bigger concurrency to make inserts faster
    int64_t original_concurrency = config.concurrency;
    if (original_concurrency < 1024) {
        config.concurrency = 1024;
    }

    // Start concurrent tasks
    std::vector<CallbackData> callbacks_data(config.concurrency);
    Semaphore finished_semaphore(config.concurrency);
    std::atomic<int64_t> next_batch_start(0);

    for (int64_t c = 0; c < config.concurrency; c++) {
        callbacks_data[c] = CallbackData {
            .config = &config,
            .session = session,
            .insert_stmt = cass_prepared_bind(prepared_insert),
            .select_stmt = nullptr, // Won't be used

            .next_batch_start = &next_batch_start,
            .cur_pk = 0,
            .end_pk = 0,

            .finished_semaphore = &finished_semaphore
        };

        finished_semaphore.acquire_permit();
        run_concurrent_task(&callbacks_data[c]);
    }

    // Wait for concurrent tasks to finish
    for (int64_t c = 0; c < config.concurrency; c++) {
        finished_semaphore.acquire_permit();
    }

    for (auto&& cb_data : callbacks_data) {
        cass_statement_free(cb_data.insert_stmt);
    }

    // Restore previous workload and conccurency
    config.workload = Workload::Selects;
    config.concurrency = original_concurrency;
}
