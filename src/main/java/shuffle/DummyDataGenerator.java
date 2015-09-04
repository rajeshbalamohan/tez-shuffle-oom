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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;

public class DummyDataGenerator extends SimpleMRProcessor {

  public DummyDataGenerator(ProcessorContext context) {
    super(context);
  }

  @Override public void run() throws Exception {
    KeyValuesWriter kvWriter = (KeyValuesWriter) (getOutputs().values().iterator().next()
        .getWriter());

    long size = 1100 * 1024 * 1024;
    System.out.println("Size : " + size);
    long recordNo = 0;
    while (size > 0) {
      LongWritable key = new LongWritable(recordNo);
      Text val = new Text(RandomStringUtils.random(100));
      kvWriter.write(key, val);
      size -= (val.getLength() + WritableUtils.getVIntSize(recordNo));
    }

  }
}
