
Disq - distributed querying

#### Architecture overview

A client distributes queries to multiple nodes.  To use this architecture, users must implement a client and a node.  The implementation must follow the requirements of disq.Client and disq.Worker, respectively.

#### Run a bunch of servers
```
	cd example
	go run node.go config-node1.json
	go run node.go config-node2.json
```

#### Run a client
```
	cd example
	go run client.go config-client.json foods.txt
```

#### Interface to node

A node must implement the disq.Worker interface. This interface requires two methods: (1) ProcessQuery(qid int, query string) string and (2) New(input_file string) disq.Worker.

Method New should return a pointer to struct of the same type.


#### Interface to client

A client must implement the disq.Client interface.  This interface requires a method called ProcessResult.