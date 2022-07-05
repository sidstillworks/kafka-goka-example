# kafka-goka-example

1. Write a service in goka to produce messages at Kafka cluster running in docker
    1. Key : number (generate randomly for each message between 0 to 5)
    2. Message structure: Two field ->
              1.  curTime (string)
              2. value (int)


2. Use AVRO as codec
3. Write a consumer in Goka, consume the messages from above topic and update group-table topic as follows for each key :
      1. Group table should contain following info for each key :
                1.  Earlier time
                2.  Latest Time
                3.  Total Sum (sum of all value fields in messages)
4. Expose an API with endpoint [GET]  /features/{key} : this will return group-table data for requested key (key comes in query param)

(Do the #1 using Sarama package)


Endpoint: http://localhost:9095/features/4   : 4 is the key for eg
