# Terms

* Commit coordinator
  * The process that is responsible for requesting data from workers and performing the Iceberg commit
* Commit worker
  * The process that is responsible for reading source messages from Kafka, converting them to Iceberg records, and writing data files
* Control topic
  * The Kafka topic used by the sink connector as a communication channel between workers and the coordinator
