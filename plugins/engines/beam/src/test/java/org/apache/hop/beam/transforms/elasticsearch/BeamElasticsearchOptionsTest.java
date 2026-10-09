/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.beam.transforms.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BeamElasticsearchOptionsTest {
  @Test
  void batchAndKeepaliveSettingsReachBeamIoAfterVariableResolution() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("BATCH", "17");
    variables.setVariable("BYTES", "4096");
    variables.setVariable("KEEPALIVE", "2m");
    BeamElasticsearchOutputMeta output = BeamElasticsearchOutputMetaTest.configured();
    BeamElasticsearchIOTest.set(output, "MaxBatchSize", "${BATCH}");
    BeamElasticsearchIOTest.set(output, "MaxBatchBytes", "${BYTES}");
    ElasticsearchIO.Write write = (ElasticsearchIO.Write) io(output, "createWrite", variables);
    assertEquals(17L, bulkOption(write, "getMaxBatchSize"));
    assertEquals(4096L, bulkOption(write, "getMaxBatchSizeBytes"));
    BeamElasticsearchInputMeta input = new BeamElasticsearchInputMeta();
    input.setHosts("http://localhost:9200");
    input.setIndex("docs");
    BeamElasticsearchIOTest.set(input, "ScrollKeepalive", "${KEEPALIVE}");
    ElasticsearchIO.Read read = (ElasticsearchIO.Read) io(input, "createRead", variables);
    assertEquals("2m", display(read, "scrollKeepalive"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "abc", "${MISSING}", "9223372036854775808"})
  void rejectsInvalidBulkLimits(String value) throws Exception {
    BeamElasticsearchOutputMeta meta = BeamElasticsearchOutputMetaTest.configured();
    BeamElasticsearchIOTest.set(meta, "MaxBatchSize", value);
    assertThrows(HopException.class, () -> io(meta, "createWrite", new Variables()));
    BeamElasticsearchIOTest.set(meta, "MaxBatchSize", "10");
    BeamElasticsearchIOTest.set(meta, "MaxBatchBytes", value);
    assertThrows(HopException.class, () -> io(meta, "createWrite", new Variables()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0m", "-1m", "", "abc", "${MISSING}"})
  void rejectsInvalidScrollKeepalive(String value) throws Exception {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts("http://localhost:9200");
    meta.setIndex("docs");
    BeamElasticsearchIOTest.set(meta, "ScrollKeepalive", value);
    assertThrows(HopException.class, () -> io(meta, "createRead", new Variables()));
  }

  static Object bulkOption(ElasticsearchIO.Write write, String name) throws Exception {
    java.lang.reflect.Method method = ElasticsearchIO.BulkIO.class.getDeclaredMethod(name);
    method.setAccessible(true);
    return method.invoke(write.getBulkIO());
  }

  static Object display(
      org.apache.beam.sdk.transforms.display.HasDisplayData transform, String key) {
    return DisplayData.from(transform).items().stream()
        .filter(i -> i.getKey().equals(key))
        .findFirst()
        .orElseThrow()
        .getValue();
  }

  static Object io(Object meta, String method, Variables variables) throws Exception {
    try {
      return meta.getClass()
          .getMethod(method, org.apache.hop.core.variables.IVariables.class)
          .invoke(meta, variables);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HopException cause) {
        throw cause;
      }
      throw e;
    }
  }
}
