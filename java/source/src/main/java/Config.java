import java.util.Arrays;

import org.apache.commons.cli.*;

class Config {

    enum Workload {
        Inserts, Selects, Mixed,
    }

    String[] node_addresses;
    Workload workload;
    long tasks;
    long concurrency;
    boolean dont_prepare;

    Config(String[] args) {

        this.node_addresses = new String[]{"127.0.0.1"};
        this.workload = Workload.Inserts;
        this.tasks = 1000 * 1000;
        this.concurrency = 1024;
        this.dont_prepare = false;

        Options options = new Options();

        options.addOption("d", "dont-prepare", false, "Don't create tables and insert into them before the benchmark");
        options.addOption("n", "nodes", true, "Addresses of database nodes to connect to separated by a comma");
        options.addOption("w", "workload", true, "Type of work to perform (Inserts, Selects, Mixed)");
        options.addOption("t", "tasks", true, "Total number of tasks (requests) to perform the during benchmark. In case of mixed workload there will be tasks inserts and tasks selects");
        options.addOption("c", "concurrency", true, "Maximum number of requests performed at once");

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("dont-prepare")) {
                this.dont_prepare = true;
            }

            if (cmd.hasOption("nodes")) {
                String value = cmd.getOptionValue("nodes");
                node_addresses = value.split(",");
            }

            if (cmd.hasOption("workload")) {
                String workloadValue = cmd.getOptionValue("workload");
                this.workload = Workload.valueOf(workloadValue);
            }

            if (cmd.hasOption("tasks")) {
                this.tasks = Integer.parseInt(cmd.getOptionValue("tasks"));
            }

            if (cmd.hasOption("concurrency")) {
                this.concurrency = Integer.parseInt(cmd.getOptionValue("concurrency"));
            }

        } catch (ParseException e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("./run.sh [OPTION]...", options);
            System.out.println();
            System.out.println("Unexpected exception: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "node_addresses=" + Arrays.toString(node_addresses) +
                ", workload=" + workload +
                ", tasks=" + tasks +
                ", concurrency=" + concurrency +
                ", dont_prepare=" + dont_prepare +
                '}';
    }
}