
Disq - distributed querying

#### Architecture overview

A client distributes queries to multiple nodes.  To use this architecture, users must implement a client and a node.  The implementation must follow the requirements of disq.Client and disq.Node, respectively.

#### Server
```
	cd example
	go run node.go config-node1.json
	go run node.go config-node2.json
```

#### Client
```
	cd example
	go run client.go config-client.json foods.txt
```

#### Interface to node

A node must implement the disq.Worker interface. This interface requires two methods: (1) ProcessQuery and (2) New, which returns a struct that satisfies the interface disq.Worker.

#### Interface to client

A client must implement the disq.Client interface.  This interface requires a method called ProcessResult.