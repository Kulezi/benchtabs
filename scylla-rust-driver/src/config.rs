use anyhow::{anyhow, Result};
use getopts::Options;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Workload {
    Inserts,
    Selects,
    Mixed,
}

#[derive(Debug)]
pub struct Config {
    pub node_addresses: Vec<String>,
    pub workload: Workload,
    pub tasks: i64,
    pub concurrency: i64,
    pub batch_size: i64,
    pub dont_prepare: bool,
}

impl Config {
    pub fn read() -> Result<Option<Config>> {
        let mut opts = Options::new();

        opts.optflag("h", "help", "Print usage information");

        opts.optopt(
            "n",
            "nodes",
            "Addresses of database nodes to connect to separated by a comma
            (default: '192.168.100.100:9042')",
            "ADDRESS",
        );
        opts.optopt(
            "w",
            "workload",
            "Type of work to perform (inserts, selects, mixed) (default: mixed)",
            "WORKLOAD",
        );
        opts.optopt("t", "tasks", "Total number of tasks (requests) to perform the during benchmark
                                In case of mixed workload there will be tasks inserts and tasks selects
                                (default: 1 000 000)", "TASKS");
        opts.optopt(
            "c",
            "concurrency",
            "Maximum number of requests performed at once
            (default: 1024)",
            "CONCURRENCY",
        );

        opts.optflag(
            "d",
            "dont-prepare",
            "Don't create tables and insert into them before the benchmark",
        );

        let args: Vec<String> = std::env::args().collect();
        let parsed = opts.parse(&args[1..])?;

        if parsed.opt_present("help") {
            println!("{}", opts.usage("Benchmark configuration"));
            return Ok(None);
        }

        let addresses: String =
            parsed.opt_get_default("nodes", "192.168.100.100:9042".to_string())?;

        let node_addresses: Vec<String> = addresses.split(',').map(|s| s.to_string()).collect();

        let workload_str: String = parsed.opt_get_default("workload", "mixed".to_string())?;
        let workload: Workload = match workload_str.as_str() {
            "inserts" => Workload::Inserts,
            "selects" => Workload::Selects,
            "mixed" => Workload::Mixed,
            other => {
                return Err(anyhow!(
                    "Invalid workload type: '{}'. Allowed values: inserts, selects, mixed",
                    other
                ))
            }
        };

        let tasks: i64 = parsed.opt_get_default("tasks", 1_000_000)?;
        let concurrency: i64 = parsed.opt_get_default("concurrency", 1024)?;

        let dont_prepare: bool = parsed.opt_present("dont-prepare");

        let mut batch_size = 256;

        if tasks / batch_size < concurrency {
            batch_size = std::cmp::max(1, tasks / concurrency);
        }

        Ok(Some(Config {
            node_addresses,
            workload,
            tasks,
            concurrency,
            batch_size,
            dont_prepare,
        }))
    }
}
