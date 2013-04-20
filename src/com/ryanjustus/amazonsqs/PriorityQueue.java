/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ryanjustus.amazonsqs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates a <code>TaskQueue</code> backed by multiple AmazonSQS queues with ascending priority numbers
 * associated with them.
 * @author ryan
 */

public class PriorityQueue implements TaskQueue{

 /**
 * A <b>PriorityTaskQueue</b> supports different priority levels for tasks. The lower the priority level the higher the precedence for getting <br />
 * returned by getTask.  Due to the nature of AmazonSQS and also for performance reasons a lower priorityLevel Task is not guaranteed to <br />
 * be retrieved before a higher priorityLevel, but in general this will be the case. <br />
 *<br />
 * \*Note: When a priority queue is created it creates/retrieves AmazonSQS queues with names name[0-level], so be sure this doesn't conflict with any existing AmazonSQS queues <br />
 *<br />
 * Basic Usage Example: <br />
 * <code> <br />
 * PriorityTaskQueue queue = new PriorityTaskQueue("testQueueName",3, "AmazonKey", "AmazonSecretKey"); <br />
 * for(int i=0;i<100;i++){ <br />
 * &nbsp;&nbsp; Map<String,String> m = new HashMap<String,String>(); <br />
 * &nbsp;&nbsp; m.put("url", "http://example.com"); <br />
 * &nbsp;&nbsp; m.put("downloadImages", "Y"); <br />
 * &nbsp;&nbsp; queue.addTask(m,i%3); <br />
 * } <br />
 * while((Task t = queue.getTask())!=null){ <br />
 * &nbsp;&nbsp;//do stuff with task <br />
 * &nbsp;&nbsp; t.completeTask(); <br />
 * } <br />
 * queue.shutdown(); <br />
 * </code> <br />
 */

    private List<BasicQueue> pq;
    private int maxNum;
    private int curLevel;
    private int defaultTimeout;
    private ScheduledThreadPoolExecutor queueMonitor;
    int time;

    public PriorityQueue(String name,int levels,String key, String secretKey)
    {
        if(levels==0 || levels>100)
        {
            throw new IllegalArgumentException("num must be between 1 and 100");
        }
        time=1;
        curLevel=0;
        defaultTimeout=180;
        maxNum=levels;
        pq = new ArrayList<BasicQueue>();
        for(int i=0;i<levels;i++){
            String queueName = name+i;
            pq.add(BasicQueue.getInstance(queueName, key, secretKey));
        }
    }

    /**
     * @return the number of priority levels specified for the queue
     */
    public int getNumLevels()
    {
        return maxNum;
    }


    /**
     * @return Task from AmazonSQS queue with the default timeout and keepAlive false
     */
    public Task getTask() {
       return getTask(defaultTimeout, false);
    }

    /**
     * Sets the time period between polls to determine the current lowest priorityNumber
     * queue that has messages. Default is 5 minutes.
     * *the lower the priorityNumber the higher the priority
     * @param minutes
     */
    public void setPollTime(int minutes)
    {
        this.time=minutes;
    }

     /**
     * @param timeout time in seconds before Task expires and is re-inserted into AmazonSQS queue
     * @param keepAlive true means that the task will renew its time with AmazonSQS if
     * it is about to expire.
     * @return  task with the specified timeout and keepAlive parameter
     */
    public synchronized Task getTask(int timeout, boolean keepAlive) {
         Task t = null;
        int level=0;
        //Start at where we think the current level is and run to the end
        System.out.println("STARTING AT LEVEL " + curLevel);
        for(int i=curLevel;i<maxNum; i++){
            if((t=pq.get(i).getTask(timeout, keepAlive))!=null){
                level=i;
                break;
            }
            System.out.println("NO TASK AT LEVEL " + i);
        }
        if(t==null && curLevel!=0){
            System.out.println("\tNO TASKS FOUND, STARTING AT 0");
            //Start at 0 and run to where we though the current level was
            for(int i=0;i<curLevel; i++){
                if((t=pq.get(i).getTask(timeout, keepAlive))!=null){
                    level=i;
                    break;
                }
                System.out.println("NO TASK AT LEVEL " + i);
            }
        }
        //if there are no tasks shutdown the queue polling.  Set the level to 0
        //for the next getTask call so it starts at the highest priority queue
        if(t==null){
            level=0;
            if(this.queueMonitor!=null)
                shutdownQueueMonitor();
        }
        //if we are on level 0 shutdown queue polling as we will start at the top
        //anyway
        else if(level==0)
        {
            if(this.queueMonitor!=null)
                shutdownQueueMonitor();
        }
        //if there are more tasks and the queue poller is shut down start it back up
        else if(level>0 && queueMonitor==null || queueMonitor.isShutdown()){
            System.out.println("PAST Queue 0, STARTING MONITOR");
            startQueuePoller();
        }
        curLevel=level;
        return t;
    }

    /**
     * start polling the queues to see the lowest priorityNum with messages
     */
    private void startQueuePoller(){
        if(queueMonitor!=null && !queueMonitor.isShutdown())
            return;
        System.out.println("testing starting queue poller");
        if(queueMonitor==null){
            queueMonitor =  new ScheduledThreadPoolExecutor(3);
            queueMonitor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
        if(queueMonitor!=null && !queueMonitor.isShutdown()) {
            System.out.println("Starting queue poller");
            queueMonitor.scheduleAtFixedRate(new QueueMonitor(), time, time, TimeUnit.MINUTES);
        }
    }


    /**
     * adds a task into the queue with priority prioityLevel
     * Lower priorityLevel's get retrieved first
     * @param attrs
     * @param priorityLevel
     */
    public void addTask(Map<String, String> attrs, int priorityLevel)
    {
        System.out.println("Pre priority level " + priorityLevel);
        if(priorityLevel<0){
            priorityLevel=0;
        }else if(priorityLevel >= maxNum) {
            priorityLevel=maxNum-1;
        }
        System.out.println("adding Task to PriorityLevel " + priorityLevel);
        BasicQueue q = pq.get(priorityLevel);
        q.addTask(attrs);
    }

    /**
     * adds a task into the priority 0 (highest priority) queue
     * @param attrs
     */
    public void addTask(Map<String, String> attrs) {
        int priority = 0;
        BasicQueue q = pq.get(priority);
        q.addTask(attrs);
    }

    /**
     * Set the default time that a Task has before AmazonSQS assumes it failed
     * and adds it back into the queue
     * @param timeout (seconds)
     */
    public void setDefaultTaskTimeout(int timeout) {
        this.defaultTimeout=timeout;
    }

    /**
     * Delete the queue from AmazonSQS
     */
    public void deleteQueue() {
        for(BasicQueue q:pq){
            q.deleteQueue();
        }
    }

    private void shutdownQueueMonitor()
    {
        System.out.println("Attempting shutdown of QueueMonitor");
        if(queueMonitor!=null){
            System.out.println("Shutting down QueueMonitor now");
            queueMonitor.shutdownNow();
            System.out.println("QueueMonitor Shut down");
        }
    }

    /**
     * Shuts down all the threads associated with the queue
     */
    public void shutdown()
    {
        shutdownQueueMonitor();
        for(BasicQueue q : pq){
            q.shutdown();
        }
        System.out.println("everything shut down");
    }

    /**
     * @return all the AmazonSQS queue urls associated with the priority queue
     */
    public List<String> listQueueUrls() {
        List<String> l = new ArrayList<String>();
        for(BasicQueue q: pq)
        {
            l.addAll(q.listQueueUrls());
        }
        return l;
    }

    /**
     * @return Approximate number of tasks in the queue
     */
    public int getNumInQueue() {
        int num=0;
        for(BasicQueue q: pq)
        {
            num+=q.getNumInQueue();
        }
        return num;
    }

    /**
     * @param priorityLevel
     * @return Approximate number of tasks in at the priorityLevel
     */
    public int getNumInPriority(int priorityLevel) {
        if(priorityLevel<0|| priorityLevel>=this.maxNum)
            throw new IllegalArgumentException("prioity out of range " + 0 + "-" + maxNum);
        return pq.get(priorityLevel).getNumInQueue();
    }

    private class QueueMonitor implements Runnable
    {
        public void run() {
            int curLevel = pollQueues();
            PriorityQueue.this.curLevel=curLevel;
        }

        private int pollQueues()
        {
            int highestNonEmptyLevel = 0;
            for(BasicQueue q: pq){
                if(q.getNumInQueue()!=0){
                    break;
                }
                highestNonEmptyLevel++;
            }
            return highestNonEmptyLevel;
        }
    }    
}