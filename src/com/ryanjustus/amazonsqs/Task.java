/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ryanjustus.amazonsqs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Represents a message in AmazonSQS
 * Retrieve a task via TaskQueue.getTask
 * @author ryan
 */
public final class Task{

    private static Gson gson = new Gson();

    private static final Type mapType = new TypeToken<Map<String,String>>(){}.getType();
    private Map<String,String> attrs;
    private String messageId;
    private String receiptHandle;
    int timeout;
    private ScheduledThreadPoolExecutor keepAliveExecutor;
    BasicQueue q;

    /**
     * Private constructor.  The public way to create a task is via TaskQueue.getTask
     * @param attrs
     */
    private Task(Map<String,String> attrs)
    {
        messageId=null;
        receiptHandle=null;
        timeout=60;
        this.attrs = new HashMap<String,String>();
        this.attrs.putAll(attrs);
    }

    /**
     * Report to AmazonSQS that the task is complete
     */
    public void completeTask() {
        if(keepAliveExecutor!=null)
            keepAliveExecutor.shutdown();
        q.deleteTask(this);
    }

    /**
     * create a task from the json, this is used by the TaskQueue when it
     * retrieves an item from AmazonSQS
     * @param json
     * @return
     */
    static Task fromJson(String json)
    {
        Map<String,String> attrMap = gson.fromJson(json,mapType);
        Task t = new Task(attrMap);
        return t;
    }

    /**
     * creates a task from attrs.  This is used by the TaskQueue when it is
     * saving a task to AmazonSQS
     * @param attrs
     * @return
     */
    static Task getInstance(Map<String,String> attrs)
    {
        return new Task(attrs);
    }

    /**
     * Cancels the task execution, adding it back into the SimpleTaskQueue
     */
    public void cancelTask()
    {
        if(keepAliveExecutor!=null)
            keepAliveExecutor.shutdown();
        completeTask();
        q.addTask(attrs);
    }
    /**
     * Retrieves the message id that AmazonSQS assigns
     * @return
     */
    String getMessageId()
    {
        return messageId;
    }

    /**
     * Set the AmazonSQS message
     * @param id
     */
    void setMessageId(String id)
    {
        messageId=id;
    }

    /**
     * Sets the AmazonSQS receiptHandler
     * @param handle
     */
    void setReceiptHandle(String handle)
    {
        this.receiptHandle=handle;
    }

    /**
     * returns the AmazonSQS receiptHandler
     * @return
     */
    String getReceiptHandle()
    {
        return receiptHandle;
    }

    /**
     * @param attr Name of the attribute
     * @return Value of the attribute
     */
    public String getAttrVal(String attr)
    {
        return attrs.get(attr);
    }

    /**
     * @return  names of all the Task attributes
     */
    public Set<String> getAttrKeys()
    {
        return attrs.keySet();
    }

    /**
     * create a JSON representation of the Task.  Used by TaskQueue when it
     * is saving the Task into AmazonSQS
     * @return
     */
    public String toJson()
    {
        return gson.toJson(attrs);
    }

    /**
     * @return Iterator over all the key/value attributes
     */
    public Iterator<Entry> getAttrValItr() {
        Map m = new TreeMap<String,String>();
        m.putAll(attrs);
        return m.entrySet().iterator();
    }

    /**
     * if keepAlive was set to true this sends a request for more time if the Task is about to expire
     * @param timeout
     */
    void keepAlive(final int timeout)
    {
        keepAliveExecutor = new ScheduledThreadPoolExecutor(1);
        keepAliveExecutor.scheduleAtFixedRate(new KeepAliveTask(this), (900*timeout), (500*timeout), TimeUnit.MILLISECONDS);
    }

    private class KeepAliveTask implements Runnable
    {
        long startTime;
        Task t;
        public KeepAliveTask(Task t)
        {
            startTime=System.currentTimeMillis();
            this.t=t;
        }

        public void run() {
           // System.out.println("keeping task alive");
            long elapsed = System.currentTimeMillis()-startTime;
            System.out.println("elapsed: " + elapsed/1000);
            System.out.println("timeout: " + timeout);
            long timeleft = 1000*timeout-elapsed;
            System.out.println("Timeleft: " +timeleft/1000);
            if(timeleft<100*timeout){
                System.out.println("*****postponing task");
                startTime=System.currentTimeMillis();
                q.requestMoreTime(t, timeout);
            }
        }
    }

    /**
     * returns the queue associated with this task
     * @return
     */
    BasicQueue getQueue(){
        return q;
    }
}
