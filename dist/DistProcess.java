import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess
implements Watcher, AsyncCallback.ChildrenCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster=false;

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 1000, this); //connect to ZK.
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			getTasks(); // Install monitoring on any new tasks that will be created.
		}catch(NodeExistsException nee)
		{
			isMaster=false;
			try
			{
				// What else will you need if this was a worker process?
				addWorker();
				// monitor for worker tasks
				getAssignedWorkers();
			} catch(NodeExistsException nee_) { }
		}

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));
	}

	// Master fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist03/tasks", this, this, null);  
	}

	// Workers fetch assigned znodes...
	void getAssignedWorkers()
	{
		zk.getChildren("/dist03/assign", this, this, null);
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist03/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	void addWorker() throws UnknownHostException, KeeperException, InterruptedException
	{
		zk.create("/dist03/workers/"+pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	void assignWorkerTask(String worker, String task) throws UnknownHostException, KeeperException, InterruptedException
	{
		byte[] taskSerial = zk.getData("/dist03/tasks/"+task, false, null);
	
		// Filter out and go over only the newly created task znodes.	
		// if data not empty	
		if (taskSerial != null && taskSerial.length != 0) {
			zk.delete("/dist03/workers/"+worker, -1);	
			zk.create("/dist03/assign/"+worker, worker.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.setData("/dist03/tasks/"+task, null, -1);	
			zk.create("/dist03/assign/"+worker+"/"+task, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);	
		}
	}

	public void process(WatchedEvent e)
	{
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);
		// Master should be notified if any new znodes are added to tasks.
		if(isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist03/tasks"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();
		}

		// Worker should be notified if any new znodes are added to asgin node.
		if(!isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist03/assign"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getAssignedWorkers();
		}
	}

	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		// This logic is for master !!
		//Every time a new task znode is created by the client, this will be invoked.

		// TODO: Filter out and go over only the newly created task znodes.
		//		Also have a mechanism to assign these tasks to a "Worker" process.
		//		The worker must invoke the "compute" function of the Task send by the client.
		//What to do if you do not have a free worker process?
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		for(String c: children)
		{
			System.out.println(c);
			try
			{
				if (isMaster) {
					// master process
					
					// wait for available worker
					List<String> workersList;
					while((workersList = zk.getChildren("/dist03/workers", false)).isEmpty()) { }
					String worker = workersList.get(0);	
					new Thread(() -> {
						try {	
							assignWorkerTask(worker, c);
						}
						catch (UnknownHostException uhe) { }
						catch (KeeperException ke) { }
						catch (InterruptedException ie) { }
					}).start();
				}
				else {
					// worker process
					// There is quite a bit of worker specific activities here,
					// that should be moved done by a process function as the worker.	
					
					if (!pinfo.equals(c)) continue;	
					
					List<String> tasksList = zk.getChildren("/dist03/assign/"+c, false);

					if (tasksList != null && !tasksList.isEmpty()) {
						System.out.println("OK!");
						// get the data using an async version of the API.
						byte[] taskSerial = zk.getData("/dist03/assign/"+c+"/"+tasksList.get(0), false, null);	
							
						// Re-construct our task object.
						ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
						ObjectInput in = new ObjectInputStream(bis);
						DistTask dt = (DistTask) in.readObject();	

						//Execute the task.
						dt.compute();	
						
						// Serialize our Task object back to a byte array!
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oos = new ObjectOutputStream(bos);
						oos.writeObject(dt); oos.flush();
						taskSerial = bos.toByteArray();	
							
						// Store it inside the result node.
						zk.create("/dist03/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						//zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						zk.create("/dist03/workers/"+pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}	
				}


			}
			catch(NodeExistsException nee){System.out.println(nee);}
			catch(KeeperException ke){System.out.println(ke);}
			catch(InterruptedException ie){System.out.println(ie);}
			catch(IOException io){System.out.println(io);}
			catch(ClassNotFoundException cne){System.out.println(cne);}
		}
	}

	public static void main(String args[]) throws Exception
	{
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
		//Thread.sleep(10000);
		while (true) { }
	}
}
