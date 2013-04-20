amazon-sqs-java-client
======================
A java client for Amazon SQS

It also supports building a priority queue on top of SQS

Basic Queue example usage:
```java
 TaskQueue queue = new BasicTaskQueue("testQueue","AmazonKey", "AmazonSecretKey");
 for(int i=0;i<100;i++){
   Map<String,String> = new HashMap<String,String>();
   m.put("url", "http://example.com");
   m.put("downloadImages", "Y");
   queue.addTask(m);
  }
  while((Task t = queue.getTask())!=null){
     //do stuff with task
     t.completeTask();
  }
  queue.shutdown();
```
