# Wordcount example with Actix, Kafka, and Redis

The wordcount example is perhaps the "hello world" of data processing 
examples. The application here shows how to stream data from Kafka into
Redis counting the individual tokens along the way. The `server` provides 
an endpoint for retrieving the counts for a specific topic. 


```bash
# starting the server (could also be cargo run --bin wordcount-server test 
$ wordcount-server test
# start the client
$ wordcount-client test-topic
<type some messages here>
# you can also pipe from stdin
# cat large-test.txt | wordcount-clinet epic-topic
```

Then you can get the counts of the words from the server like so:

```bash
# assuming you have started wordcount-server
$ curl http://127.0.0.1:8080/counts?topic=epic-topic
# to get the top N (10 in this example) counts
$ curl 'http://127.0.0.1:8080/counts?topic=epic-topic&n=10'
```

## Setup
The required services (Redis and Kafka) can be started with the included 
`docker-compose` file
```bash
docker-compose up
```

The two executables can be built with `cargo` as you would expect
```bash
$ cargo build <--release>
```
Then started accordingly
```bash
$ target/<debug|release>/wordcount-server <topic-name> <topic-name> ..
$ target/<debug|release>/wordcount-client <topic-name>
```


