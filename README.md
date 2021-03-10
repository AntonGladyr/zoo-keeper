# ZooKeeper & Distributed Computing

## Overview

A simple distributed computing platform following a master-worker(s) architecture that uses ZooKeeper for coordination. The high-level objective of the platform is that a
client can submit a "task/job" for computation to the platform, which will execute the task/job using one of its many workers and provide the client with the result. For the purpose of this project, we do
not consider crashes and failovers of the member processes and assume that all processes gracefully terminate.

The _task_ directory contains the definitions of "Jobs" classes that the client programs can construct an object and (serialize) submit to the distributed platform through the ZooKeeper. For
the purpose of this project, we have a Monte Carlo based computation of the value of __pi__ defined in MCPi.java.

The _clnt_ directory contains the client java program (DistClient.java) that creates the "job" object and submits it to the platform, awaits the result and retrieves it. It does this by creating
a sequential znode /distXX/tasks/task- that contains the "job" object (serialized) as the data and then waiting for a result znode to be created under it.

The dist directory contains DistProcess.java that defines the program code that all the members in the distributed platform have.

## Usage

You can invoke the client in the following way (make sure that the ZOOBINDIR is set and the env script is executed.)

```$ ./runclnt.sh 500000```

Where 500000 is the number of samples (points) that the Monte Carlo simulation should use (the larger the number, the more computation it needs to perform, but the more accurate the
computed value of pi would be).

A member process can be started as follows:

```./runsrvr.sh```

The general idea to start a process from a different machine.(And therefore, will have multiple members in the distributed platform).

