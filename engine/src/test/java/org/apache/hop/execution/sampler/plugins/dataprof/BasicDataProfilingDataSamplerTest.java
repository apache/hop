/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution.sampler.plugins.dataprof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.plugins.dataprof.BasicDataProfilingDataSampler.ProfilingType;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BasicDataProfilingDataSamplerTest {

  @BeforeAll
  static void before() throws Exception {
    HopClientEnvironment.init();
    TestUtil.registerTestPluginTypes();
  }

  @Test
  void avroColumnIsProfiledAndSurvivesExecutionData() throws Exception {
    BasicDataProfilingDataSampler sampler = new BasicDataProfilingDataSampler();
    sampler.setSampleSize("10");
    sampler.setOnlyProfilingLastTransforms(true);
    ExecutionDataSamplerMeta samplerMeta =
        new ExecutionDataSamplerMeta("Kafka consumer", "0", "log-channel", false, true);
    BasicDataProfilingDataSamplerStore store = sampler.createSamplerStore(samplerMeta);
    store.init(new Variables(), null, null);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaAvroRecord("message"));

    GenericRecord first = record("alpha");
    GenericRecord second = record("beta");
    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, new Object[] {"1", first});
    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, new Object[] {"2", second});

    assertNotNull(store.getMinValues().get("message"));
    assertNotNull(store.getMaxValues().get("message"));

    sampler.sampleRow(store, IStream.StreamType.OUTPUT, rowMeta, new Object[] {"3", null});

    assertEquals(1L, store.getNullCounters().get("message"));
    assertEquals(2L, store.getNonNullCounters().get("message"));
    assertNotNull(store.getMaxValues().get("message"));

    RowBuffer nonNullSamples =
        store.getProfileSamples().get("message").get(ProfilingType.NrNonNulls);
    assertNotSame(first, nonNullSamples.getBuffer().get(0)[1]);

    ExecutionData data =
        ExecutionDataBuilder.of()
            .withParentId("parent")
            .withOwnerId(ExecutionDataBuilder.ALL_TRANSFORMS)
            .addDataSets(store.getSamples())
            .addSetMeta(store.getSamplesMetadata())
            .build();

    ObjectMapper mapper = HopJson.newMapper();
    ExecutionData copy = mapper.readValue(mapper.writeValueAsString(data), ExecutionData.class);
    assertEquals(data, copy);
  }

  private static GenericRecord record(String body) {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("body", new Utf8(body));
    return record;
  }
}
