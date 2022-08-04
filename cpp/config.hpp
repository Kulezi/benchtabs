#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <stdio.h>

enum class Workload {
    Inserts,
    Selects,
    Mixed,
};

inline void print_usage();

struct Config {
    std::vector<std::string> node_addresses;
    Workload workload;
    int64_t tasks;
    int64_t concurrency;
    int64_t batch_size;
    bool dont_prepare;

    Config(int argc, const char *argv[]) {
        node_addresses = {"127.0.0.1"};
        workload = Workload::Inserts;
        tasks = 1000 * 1000;
        concurrency = 1024;
        dont_prepare = false;

        for (int a = 1; a < argc;) {
            // --dont-prepare
            if (strcmp(argv[a], "-d") == 0 || strcmp(argv[a], "--dont-prepare") == 0) {
                dont_prepare = true;
                a += 1;
                continue;
            }

            if (a + 1 >= argc) {
                fprintf(stderr, "ERROR: No value specified for argument %s!\n", argv[a]);
                std::exit(1);
            }

            // --nodes
            if (strcmp(argv[a], "-n") == 0 || strcmp(argv[a], "--nodes") == 0) {
                std::vector<std::string> addresses = {""};

                for (const char* c = argv[a + 1]; *c != 0; c++) {
                    if (*c == ',') {
                        addresses.push_back("");
                    } else {
                        addresses.back() += *c;
                    }
                }

                node_addresses = addresses;

                a += 2;
                continue;
            }

            // --workload
            if (strcmp(argv[a], "-w") == 0 || strcmp(argv[a], "--workload") == 0) {
                if (strcmp(argv[a + 1], "inserts") == 0) {
                    workload = Workload::Inserts;
                } else if (strcmp(argv[a + 1], "selects") == 0) {
                    workload = Workload::Selects;
                } else if (strcmp(argv[a + 1], "mixed") == 0) {
                    workload = Workload::Mixed;
                } else {
                    fprintf(stderr,
                            "ERROR: Invalid workload specified: %s. (Allowed values: inserts, "
                            "selects, mixed)\n",
                            argv[a + 1]);
                    
                    std::exit(1);
                }

                a += 2;
                continue;
            }

            // --tasks
            if (strcmp(argv[a], "-t") == 0 || strcmp(argv[a], "--tasks") == 0) {
                tasks = std::strtoll(argv[a + 1], nullptr, 10);

                a += 2;
                continue;
            }

            // --concurrency
            if (strcmp(argv[a], "-c") == 0 || strcmp(argv[a], "--concurrency") == 0) {
                concurrency = std::strtoll(argv[a + 1], nullptr, 10);

                a += 2;
                continue;
            }

            fprintf(stderr, "Unkown argument: %s\n", argv[a]);
            std::exit(1);
        }

        batch_size = 256;

        if (tasks/batch_size < concurrency) {
            batch_size = std::max((int64_t)1, tasks / concurrency);
        }
    }

    void print() {
        std::cerr << "Config:\n";
        std::cerr << "    nodes: [";
        for (auto&& node_address : node_addresses) {
            std::cerr << node_address << ", ";
        } 
        std::cerr << "]\n";

        std::cerr << "    workload: ";

        switch (workload) {
        case Workload::Inserts:
            std::cerr << "Inserts\n";
            break;

        case Workload::Selects:
            std::cerr << "Selects\n";
            break;

        case Workload::Mixed:
            std::cerr << "Mixed\n";
            break;
        }

        std::cerr << "    tasks: " << tasks << "\n";
        std::cerr << "    concurrency: " << concurrency << "\n";
        std::cerr << "    dont_prepare: " << (dont_prepare ? "true" : "false") << "\n";

        std::cerr << std::endl;
    }
};

#endif