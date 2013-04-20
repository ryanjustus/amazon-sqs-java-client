/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ryanjustus.amazonsqs;

import java.util.List;
import java.util.Map;

/**
 *
 * @author ryan
 */
public interface TaskQueue {

    /**
     * Sets the default time(seconds) for time before AmazonSQS assumes the
     * task failed and returns it to the queue. If this hasn't been set the timeout
     * is 180 seconds
     */
    public void setDefaultTaskTimeout(int timeout);
    /**
     * @return all the queue urls associated with the queue
     */
    public List<String> listQueueUrls();
    /**
     * Deletes the queue from AmazonSQS
     */
    public void deleteQueue();
    /**
     * @return  Approximate number of items in the queue
     */
    public int getNumInQueue();
    /**
     * add a Task to the TaskQueue
     * @param attrs Task attributes
     */
    public void addTask(Map<String,String>attrs);
    /**
     * retrieves a task from the TaskQueue with the default timeout and keepAlive false
     * @return Task from AmazonSQS queue
     */
    public Task getTask();
    /**
     * retrieves a task with the specified timeout and keepAlive.
     * keepAlive=true means that the task will renew its time with AmazonSQS if
     * it is about to expire.
     * @param timeout
     * @param keepAlive
     * @return Task from AmazonSQS queue
     */
    public Task getTask(int timeout, boolean keepAlive);
    /**
     * Shutdown all the threads associated with the queue.
     */
    public void shutdown();

}
