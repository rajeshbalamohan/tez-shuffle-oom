- Intention is to reproduce OOM with shuffle (Scatter Gather)
- Have a Vertex with DummyDataGenerator (to generate junk data of size ~ 1.1 GB from every task). Have 10 tasks.
- Have another vertex with parallelism set to 1 & with scatter gather edge.
- Code automatically sets the resource requirement to 10 GB per task (10 GB container)

Build & Run:
===========
1. mvn clean package
2. hadoop dfs -put ./target/tez-shuffle-oom-1.0-SNAPSHOT.jar /tmp/
3. yarn jar tez-shuffle-oom-1.0-SNAPSHOT.jar shuffle.ShuffleCheck -Dtez.runtime.pipelined-shuffle.enabled=false -Dtez.runtime.shuffle.merge.percent=0.9 -Dtez.runtime.shuffle.memory.limit.percent=0.25 -Dmapred.job.reduce.input.buffer.percent=0.0 -Dmapred.job.shuffle.input.buffer.percent=0.9 hdfs://cn041-10.l42scl.hortonworks.com:8020/tmp/tez-shuffle-oom-1.0-SNAPSHOT.jar /tmp/del11

Expected Exception:
==================
], TaskAttempt 3 failed, info=[Error: Failure while running task: org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$ShuffleError: error in shuffle in Fetcher [source] #5
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$RunShuffleCallable.callInternal(Shuffle.java:301)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$RunShuffleCallable.callInternal(Shuffle.java:285)
        at org.apache.tez.common.CallableWithNdc.call(CallableWithNdc.java:36)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.OutOfMemoryError: Java heap space
        at org.apache.hadoop.io.BoundedByteArrayOutputStream.<init>(BoundedByteArrayOutputStream.java:56)
        at org.apache.hadoop.io.BoundedByteArrayOutputStream.<init>(BoundedByteArrayOutputStream.java:46)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.<init>(MapOutput.java:81)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.createMemoryMapOutput(MapOutput.java:133)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager.unconditionalReserve(MergeManager.java:441)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager.reserve(MergeManager.java:429)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.copyMapOutput(FetcherOrderedGrouped.java:442)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.copyFromHost(FetcherOrderedGrouped.java:262)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.fetchNext(FetcherOrderedGrouped.java:165)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.callInternal(FetcherOrderedGrouped.java:178)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.callInternal(FetcherOrderedGrouped.java:54)
        ... 5 more
, errorMessage=Shuffle Runner Failed:org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$ShuffleError: error in shuffle in Fetcher [source] #5
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$RunShuffleCallable.callInternal(Shuffle.java:301)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.Shuffle$RunShuffleCallable.callInternal(Shuffle.java:285)
        at org.apache.tez.common.CallableWithNdc.call(CallableWithNdc.java:36)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
Caused by: java.lang.OutOfMemoryError: Java heap space
        at org.apache.hadoop.io.BoundedByteArrayOutputStream.<init>(BoundedByteArrayOutputStream.java:56)
        at org.apache.hadoop.io.BoundedByteArrayOutputStream.<init>(BoundedByteArrayOutputStream.java:46)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.<init>(MapOutput.java:81)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.createMemoryMapOutput(MapOutput.java:133)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager.unconditionalReserve(MergeManager.java:441)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager.reserve(MergeManager.java:429)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.copyMapOutput(FetcherOrderedGrouped.java:442)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.copyFromHost(FetcherOrderedGrouped.java:262)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.fetchNext(FetcherOrderedGrouped.java:165)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.callInternal(FetcherOrderedGrouped.java:178)
        at org.apache.tez.runtime.library.common.shuffle.orderedgrouped.FetcherOrderedGrouped.callInternal(FetcherOrderedGrouped.java:54)
        ... 5 more

Reason:
======
JVM heap space has young and old generation. When we have all in-memory fetches (which are in the border line of 1.1 GB; i.e max single fetch limit), we easily run into the borderline of old-gen capacity. JVM would not be able to allocate more mem in the old-gen (~5.5 GB in this case with 8 GB JVM).
This leads to the OOM. Easy option would be reduce "tez.runtime.shuffle.merge.percent".

With pipelinedshuffle this might not happen very frequently; Reason is that, with pipelined shuffle, data is sent to downstream vertex when spill happens (Note that pipelinedsorter can have more smaller spills in parallel). This would reduce the possibility of in-memory merges reaching the 1.1 GB threshold all times.
