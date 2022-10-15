# confluent-pc-tools
Simple tools for Confluent Parallel Consumer

#KafkaPcGroups

Tool to query PC metadata for more accurate reporting of the consumer lag

build:
```
mvn package
```
usage:

```
Usage: kafkapcgroups [-hV] --bootstrap-server=<bootstrap>
                     [--command-config=<configFile>] [--timeout=<timeout>]
                     (--group=<groupid> | --all-groups)
Prints highest seen offset and incomplete offsets for parallel consumer groups.
      --all-groups          Apply to all consumer groups.
      --bootstrap-server=<bootstrap>
                            REQUIRED: The server(s) to connect to.
      --command-config=<configFile>
                            Property file containing configs to be passed to
                              Admin Client and Consumer.
      --group=<groupid>     The consumer group we wish to act on.
  -h, --help                Show this help message and exit.
      --timeout=<timeout>   The timeout that can be set for someuse cases. For
                              example, it can be used when describing the group
                              to specify the maximum amount of time in
                              milliseconds to wait before the group stabilizes
                              (when the group is just created, or is going
                              through some changes). (default: 15000)
  -V, --version             Print version information and exit.
```

run:
```
java - jar <jar you just built> <properties as per above
```

Output:
```
HIGHEST-OFFSET - highest offset read by the Parallel Consumer
ADJUSTED-LAG   - lag adjusted based on the number of incomplete offsets
INCOMPLETE-ID  - list of incomplete offsets
```
The  rest is the same as for the kafka-consumer-groups 

