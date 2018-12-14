# fileopsdaemon-go
High performance, Golang based Linux daemon for working with filesystems, including NFSv4 ACLs. Communicates via Unix Domain Socket to avoid the security hazards and overhead of having user facing applications "shell out" to OS utilties or have permission to make sweeping changes to file systems.

The system can be used as a daemon process that listens on a Unix Domain Socket or as one-off direct invocations of it's binary. 

Capable of recursively setting NSFv4 ACLs with comparable speed to a simple `chmod -R` !

### Prerequisites

What things you need to install the software and how to install them

```
libzmq5
libzmq3-dev
```

On ubuntu simpliy `apt install libzmq5 libzmq3-dev`
