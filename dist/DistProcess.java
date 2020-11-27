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
// Done: Replace XX with your group number.
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
		
		// Try to become the master
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			getTasks();     // Install monitoring on any new tasks that will be created
			getWorkers();   // Install monitoring on any new workers that become available
		}
		// Upon failure becoming the master, become a worker instead
		catch(NodeExistsException nee)
		{
			isMaster=false;
			try
			{
				addWorker();      // Register as a worker
				getAssignments(); // Install monitoring on task assignments for this worker
			}
			catch(NodeExistsException nee_)
			{
				System.out.println("DISTAPP : An error occured while registering as a worker");
				System.out.println(nee);
			}
		}

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));
	}

	// Master gets updates for new task znodes
	void getTasks()
	{
		zk.getChildren("/dist03/tasks", this, this, "tasks");  
	}
	
	// Master gets updates for new worker znodes
	void getWorkers()
	{
		zk.getChildren("/dist03/workers", this, this, "workers");
	}

	// Workers gets updates for new assignment znodes
	void getAssignments()
	{
		zk.getChildren("/dist03/assign/"+pinfo, this, this, "assign");
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist03/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	// Creates an ephemeral node for this worker indicating that the worker is available for assignment
	void addWorker() throws UnknownHostException, KeeperException, InterruptedException
	{
		zk.create("/dist03/workers/"+pinfo, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	// Assigns a task to the given worker
	void assignWorkerTask(String worker, String task) throws UnknownHostException, KeeperException, InterruptedException
	{
		System.out.println("DISTAPP : assigning task : " + task + " to worker " + worker);
		
		byte[] taskSerial = zk.getData("/dist03/tasks/"+task, false, null);
	
		// if data not empty	
		if (taskSerial != null && taskSerial.length != 0) {
			
			// Delete the node indicating that the worker is free
			zk.delete("/dist03/workers/"+worker, -1);
			
			// Create an assignment node for the worker's new task
			zk.create("/dist03/assign/"+worker, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/dist03/assign/"+worker+"/"+task, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			
			// Mark the node as in-progress under "tasks"
			zk.create("/dist03/tasks/"+task, "in-progress".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			System.out.println("DISTAPP : task assigned");
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
			System.out.println("DISTAPP : Processing event: tasks watch triggered");
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();
		}

		// Worker should be notified if any new znodes are added to its assign node.
		if(!isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist03/assign/"+pinfo))
		{
			System.out.println("DISTAPP : Processing event: assignment watch triggered");
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getAssignments();
		}
	}

	// Asynchronous callback that is invoked by the zk.getChildren request.
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
		
		// Convert the context to a string
		String context = (String) ctx;
		
		for(String c: children)
		{
			System.out.println(c);
			try
			{
				if (isMaster) {
					// Master process
					
					// The master may receive a task
					if (context.equals("tasks"))
					{
						String task = c;
						
						// Check whether this task is in progress
						// If the task is not in progress, search for a worker to which to assign the task
						if (!taskIsInProgress(task))
						{
							System.out.println("DISTAPP : processing new task : " + task);
							
							// Check if there are any available workers
							List<String> workers = zk.getChildren("/dist03/workers", false);
							if (workers.size() > 0)
							{
								// If a worker is available, assign the task to it
								String worker = workers.get(0);
								System.out.println("DISTAPP : assigning task " + task + " to worker " + worker);
								assignWorkerTask(worker, task);
							}
							// Otherwise, ignore the task for now
						}
					}
					// The master may receive a worker
					else if (context.equals("workers"))
					{
						String worker = c;
						
						System.out.println("DISTAPP : processing new worker : " + worker);
						
						// Search for a task to assign to this worker
						List<String> tasks = zk.getChildren("/dist03/tasks", false);
						String chosenTask = null;
						
						for (String task : tasks)
						{
							// Check whether the task is in progress
							if (!taskIsInProgress(task))
							{
								// If it isn't, select it
								chosenTask = task;
								break;
							}
						}
						
						// If a free task exists, assign it to the worker
						if (chosenTask != null) assignWorkerTask(worker, chosenTask);
						
						// Otherwise, ignore the worker for now
					}
				}
				else {
					// Worker process
					
					// Workers receive assignments for them
					if (context.equals("assign"))
					{
						String task= c;
						
						System.out.println("DISTAPP : received new task assignment : " + task);
						
						// Execute the expensive computation in a new thread
	                    new Thread(() -> {
	                    	try
	                    	{
	                    		// Get the data for the task
								byte[] taskSerial = zk.getData("/dist03/assign/"+pinfo+"/"+c, false, null);
								
								// Re-construct our task object
								ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
								ObjectInput in = new ObjectInputStream(bis);
								DistTask dt = (DistTask) in.readObject();
		                    	
		                    	dt.compute();
		                    	
		                    	// Serialize our Task object back to a byte array!
								ByteArrayOutputStream bos = new ByteArrayOutputStream();
								ObjectOutputStream oos = new ObjectOutputStream(bos);
								oos.writeObject(dt); oos.flush();
								taskSerial = bos.toByteArray();	
									
								// Store it inside the result node.
								zk.create("/dist03/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
								
								// Delete the task assignment
								zk.delete("/dist03/assign/"+pinfo+"/"+task, -1);
								zk.delete("/dist03/assign/"+pinfo, -1);

								// Re-add this worker to the list of available workers
								zk.create("/dist03/workers/"+pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	                    	}
	                    	catch(Exception e){System.out.println(e);}
                        });
					}
				}


			}
			catch(Exception e){System.out.println(e);}
		}
	}
	
	// Returns whether a given task is currently in progress
	private boolean taskIsInProgress(String task)
	{
		List<String> taskChildren = zk.getChildren("/dist03/tasks/"+task, false);
		
		for (String child : taskChildren) if (child.equals("in-progress")) return true;
		return false;
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
