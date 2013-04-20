/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ryanjustus.amazonsqs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents an AmazonSQS queue
 *<br />
 * @author ryan
 */
public class BasicQueue implements TaskQueue{

 /** Basic Usage Example: <br />
 * <code><br />
 * TaskQueue queue = new BasicTaskQueue("testQueue","AmazonKey", "AmazonSecretKey");<br />
 * for(int i=0;i<100;i++){<br />
 * &nbsp;&nbsp; Map<String,String> = new HashMap<String,String>();<br />
 * &nbsp;&nbsp; m.put("url", "http://example.com");<br />
 * &nbsp;&nbsp; m.put("downloadImages", "Y");<br />
 * &nbsp;&nbsp; queue.addTask(m);<br />
 * }<br />
 * while((Task t = queue.getTask())!=null){<br />
 * &nbsp;&nbsp; //do stuff with task<br />
 * &nbsp;&nbsp; t.completeTask();<br />
 * }<br />
 * queue.shutdown();<br />
 * </code>
 */

    private static Map<String,BasicQueue> queues = new HashMap<String,BasicQueue>();
    private static BasicQueue q;
    private static AmazonSQS sqs;
    private int defaultTimeout;

    private String queueUrl;

    /**
     * @param queueName AmazonSQS queue name
     * @param awsKey Amazon key
     * @param secretKey Amazon secretKey
     * @return BasicTaskQueue associated with the AmazonSQS task queue
     */
    public static BasicQueue getInstance(String queueName, String awsKey, String secretKey){
        if(sqs==null){
             sqs = new AmazonSQSClient(new BasicAWSCredentials(awsKey,secretKey));
        }
        if(queues.get(queueName)==null){
            q = new BasicQueue(queueName);
            queues.put(queueName, q);
        }
        return queues.get(queueName);
    }
    


    private BasicQueue (String queueName){
        defaultTimeout=180;
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        //set some default values
        int messageTimeout=180;
        int messageLifetime=1209600;
        SetQueueAttributesRequest qa = new SetQueueAttributesRequest();
        Map<String,String> attrs = new TreeMap<String,String>();
        attrs.put("MessageRetentionPeriod", String.valueOf(messageLifetime));
        attrs.put("VisibilityTimeout", String.valueOf(messageTimeout));
        qa.setAttributes(attrs);
        qa.setQueueUrl(queueUrl);
        sqs.setQueueAttributes(qa);
    }
    
    /**
     * Sets the amount of time the service has to complete the task in seconds
     * Default is 180 (3 minutes)
     * @param timeout
     */
    public void setDefaultTaskTimeout(int timeout) {
        defaultTimeout=timeout;
    }

    /**
     * @return List of urls associated with this queue.  For a BasicTaskQueue this
     * List will only contain 1 url.
     */
    public List<String> listQueueUrls() {
        return sqs.listQueues().getQueueUrls();
    }

    /**
     * Deletes this queue from AmazonSQS
     */
    public void deleteQueue() {
        deleteQueue(queueUrl);
    }


    /**
     * Deletes the AmazonSQS queue with the associated url
     * @param queueUrl
     */
    void deleteQueue(String queueUrl){
        DeleteQueueRequest d = new DeleteQueueRequest(queueUrl);
        sqs.deleteQueue(d);
    }

    /**
     * @return all the queue urls associated with the queue
     */
    public String getQueueUrl() {
        return queueUrl;
    }

    /**
     * @return Approximate number of tasks remaining
     */
    public int getNumInQueue() {
        Set<String> attrs = new HashSet<String>();
        attrs.add("ApproximateNumberOfMessages");
        GetQueueAttributesRequest a = new GetQueueAttributesRequest().withQueueUrl(queueUrl).withAttributeNames(attrs);
        Map<String,String> result = sqs.getQueueAttributes(a).getAttributes();
        int num = Integer.parseInt(result.get("ApproximateNumberOfMessages"));
        return  num;
    }



    /**
     * Add a task to the TaskQueue
     * @param attrs
     */
    public void addTask(Map<String,String>attrs)
    {
        final Map m = new TreeMap<String,String>();
        m.putAll(attrs);
        Task t = Task.getInstance(m);
        SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(t.toJson());
        req.setMessageBody(t.toJson());
        sqs.sendMessage(req);
    }

    /**
     * retrieves a task from the TaskQueue with the default timeout and keepAlive false
     * @return Task from AmazonSQS queue
     */
    public Task getTask()
    {
        return getTask(defaultTimeout, false);
    }

    /**
     * retrieves a task with the specified timeout and keepAlive.
     * keepAlive=true means that the task will renew its time with AmazonSQS if
     * it is about to expire.
     * @param timeout
     * @param keepAlive
     * @return Task from AmazonSQS queue
     */
    public Task getTask(int timeout, boolean keepAlive)
    {
        Task t = null;
        ReceiveMessageRequest req = new ReceiveMessageRequest(queueUrl);
        req.setVisibilityTimeout(timeout);
        req.setMaxNumberOfMessages(1);
        List<Message> messages = sqs.receiveMessage(req).getMessages();
        for(Message m: messages)
        {
            t = Task.fromJson(m.getBody());
            t.setMessageId(m.getMessageId());
            t.setReceiptHandle(m.getReceiptHandle());
            t.q=this;
            t.timeout=timeout;
            if(keepAlive){
                t.keepAlive(timeout);
            }
        }
        return t;
    }
    
    /**
     * Deletes the task from the queue.  This is pubicly called by Task.completeTask
     * @param t Task to delete
     */
    void deleteTask(Task t)
    {
        final String receiptHandle = t.getReceiptHandle();
         DeleteMessageRequest d = new DeleteMessageRequest(queueUrl,receiptHandle);
         sqs.deleteMessage(d);
    }
    
    /**
     * Add time onto the task request
     * @param t
     * @param extension resets task timeout to extension (seconds)
     */
    void requestMoreTime(Task t, final int extension)
    {
        final String receiptHandle = t.getReceiptHandle();
         ChangeMessageVisibilityRequest c = new ChangeMessageVisibilityRequest(queueUrl, receiptHandle, extension);
         sqs.changeMessageVisibility(c);
 
    }

    /**
     * Shutdown all threads associated with this queue
     */
    public void shutdown()
    {
        sqs.shutdown();
    }
}