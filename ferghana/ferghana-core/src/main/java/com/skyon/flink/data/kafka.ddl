CREATE TABLE kafkaSourceTable (
  product_id INT,
  product STRING,
  amount INT
) WITH (
  'connector.type' = 'kafka',

  'connector.version' = '0.10',     -- required: valid connector versions are
                                    -- "0.8", "0.9", "0.10", "0.11", and "universal"

  'connector.topic' = 'kafkaSourceTable', -- required: topic name from which the table is read

  'connector.properties.zookeeper.connect' = 'localhost:2181', -- required: specify the ZooKeeper connection string
  'connector.properties.bootstrap.servers' = 'localhost:9092', -- required: specify the Kafka server connection string
  'connector.properties.group.id' = 'testGroup', --optional: required in Kafka consumer, specify consumer group
  'connector.startup-mode' = 'earliest-offset',    -- optional: valid modes are "earliest-offset",
                                                   -- "latest-offset", "group-offsets",
                                                   -- or "specific-offsets"

  'format.type' = 'json'                 -- required: Kafka connector requires to specify a format,
                                         -- the supported formats are 'csv', 'json' and 'avro'.
                                         -- Please refer to Table Formats section for more details.
)


CREATE TABLE kafkaSinkTable (
  product_id INT,
  product STRING,
  amount INT
) WITH (
  'connector.type' = 'kafka',

  'connector.version' = '0.10',     -- required: valid connector versions are
                                    -- "0.8", "0.9", "0.10", "0.11", and "universal"

  'connector.topic' = 'kafkaSinkTable', -- required: topic name from which the table is read

  'connector.properties.zookeeper.connect' = 'localhost:2181', -- required: specify the ZooKeeper connection string
  'connector.properties.bootstrap.servers' = 'localhost:9092', -- required: specify the Kafka server connection string

  'connector.sink-partitioner' = 'round-robin',  -- optional: output partitioning from Flink's partitions
                                         -- into Kafka's partitions valid are "fixed"
                                         -- (each Flink partition ends up in at most one Kafka partition),
                                         -- "round-robin" (a Flink partition is distributed to
                                         -- Kafka partitions round-robin)
                                         -- "custom" (use a custom FlinkKafkaPartitioner subclass)

  'format.type' = 'json'                 -- required: Kafka connector requires to specify a format,
                                         -- the supported formats are 'csv', 'json' and 'avro'.
                                         -- Please refer to Table Formats section for more details.
)