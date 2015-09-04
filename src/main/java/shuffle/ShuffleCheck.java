/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package shuffle;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 *   PLEASE IGNORE ALL BAD CODING PRACTICE HERE. Intention is to reproduce the OOM. :)
 *
 *   Run as:
 *
 *   hadoop dfs -put ./target/tez-shuffle-oom-1.0-SNAPSHOT.jar /tmp/
 *
 *   yarn jar tez-shuffle-oom-1.0-SNAPSHOT.jar shuffle.ShuffleCheck -Dtez.runtime.pipelined-shuffle.enabled=false
 *     -Dtez.runtime.shuffle.merge.percent=0.9 -Dtez.runtime.shuffle.memory.limit.percent=0.25
 *    -Dmapred.job.reduce.input.buffer.percent=0.0 -Dmapred.job.shuffle.input.buffer.percent=0.9
 *    hdfs://nn:8020/tmp/tez-shuffle-oom-1.0-SNAPSHOT.jar /tmp/del11
 *
 * </pre>
 */
public class ShuffleCheck extends Configured implements Tool {

  @Override public int run(String[] args) throws Exception {
    String dagName = "ShuffleCheck";
    DAG dag = DAG.create(dagName);

    TezConfiguration tezConf = new TezConfiguration(getConf());

    System.out.println(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT + "=" +
        tezConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT_DEFAULT));

    System.out.println(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + "=" +
        tezConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT));

    System.out.println(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + "=" +
        tezConf.getFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT));

    //Location of the custom resources
    Path path = new Path(args[0]);
    FileSystem fs = FileSystem.get(getConf());

    Map<String, LocalResource> resources = Maps.newHashMap();
    resources.put("job.jar", createLocalResource(fs, path));

    dag.addTaskLocalFiles(resources);

    //Request for 10 GB container. (parallelism set to 10 for generating junk data)
    Vertex sourceVertex = Vertex.create("source",
        ProcessorDescriptor.create(DummyDataGenerator.class.getName()), 10,
        Resource.newInstance(10240, 1));
    dag.addVertex(sourceVertex);
    System.out.println("Added source vertex");

    String outputFile = args[1]; //Some junk output file

    //Request for 10 GB container
    Vertex targetVertex = Vertex.create("destination", ProcessorDescriptor.create(EchoProcessor
        .class.getName()), 1, Resource.newInstance(10240, 1)).addDataSink("output",
        MROutput.createConfigBuilder(tezConf,
            TextOutputFormat.class, outputFile).build());
    dag.addVertex(targetVertex);
    System.out.println("Created target Vertex");

    Edge sourceToTarget =
        Edge.create(sourceVertex, targetVertex, OrderedPartitionedKVEdgeConfig.newBuilder
            (Text.class.getName(), Text.class.getName(), HashPartitioner.class.getName())
            .setFromConfiguration(tezConf).build().createDefaultEdgeProperty());
    dag.addEdge(sourceToTarget);

    TezClient client = TezClient.create(dagName, tezConf);
    client.start();
    client.waitTillReady();

    DAGClient dagClient = client.submitDAG(dag);
    Set<StatusGetOpts> getOpts = Sets.newHashSet();
    getOpts.add(StatusGetOpts.GET_COUNTERS);

    DAGStatus dagStatus;
    dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

    System.out.println(dagStatus.getDAGCounters());
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;
  }

  LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {
    final LocalResourceType type = LocalResourceType.FILE;
    final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    FileStatus fstat = fs.getFileStatus(file);
    org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);
    return lr;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new ShuffleCheck(), args);
  }
}
