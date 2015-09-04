package shuffle;

import com.google.common.base.Preconditions;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

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
public class EchoProcessor extends SimpleMRProcessor {

  public EchoProcessor(ProcessorContext context) {
    super(context);
  }

  @Override public void run() throws Exception {
    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getOutputs().size() == 1);

    LogicalInput input = getInputs().values().iterator().next();
    LogicalOutput output = getOutputs().values().iterator().next();

    Reader r = input.getReader();
    if (r instanceof KeyValuesReader) {
      KeyValuesReader reader = (KeyValuesReader) r;
      KeyValueWriter writer = (KeyValueWriter) output.getWriter();

      while (reader.next()) {
        Object key = reader.getCurrentKey();
        for (Object val : reader.getCurrentValues()) {
          writer.write(key, val);
        }
      }
    } else if (r instanceof KeyValueReader) {
      KeyValueReader reader = (KeyValueReader) r;
      KeyValueWriter writer = (KeyValueWriter) output.getWriter();

      while (reader.next()) {
        writer.write(reader.getCurrentKey(), reader.getCurrentValue());
      }
    }

  }
}
