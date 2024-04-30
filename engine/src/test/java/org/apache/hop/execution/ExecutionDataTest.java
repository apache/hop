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
 *
 */

package org.apache.hop.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.junit.Before;
import org.junit.Test;

public class ExecutionDataTest {

  @Before
  public void before() throws Exception {
    // Load data type plugins
    HopClientEnvironment.init();
  }

  @Test
  public void testSerialization() throws Exception {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addInteger("id", 9)
            .addString("firstName", 35)
            .addString("lastName", 35)
            .addBoolean("enabled")
            .addNumber("someNumber", 7, 3)
            .addBigNumber("bigNumber", 25, 10)
            .addDate("logDate")
            .build();
    List<Object[]> rows =
        Arrays.asList(
            new Object[] {
              1L,
              "Apache",
              "Hop",
              true,
              987.654,
              new BigDecimal("132384738943236.2345678901"),
              new Date()
            },
            new Object[] {
              2L,
              "Apache",
              "Beam",
              false,
              876.543,
              new BigDecimal("132344728963226.2325679902"),
              new Date()
            },
            new Object[] {
              3L,
              "Apache",
              "Spark",
              true,
              765.432,
              new BigDecimal("231385738943236.1325658801"),
              new Date()
            },
            new Object[] {
              4L,
              "Apache",
              "Flink",
              false,
              654.321,
              new BigDecimal("290375731946235.9325653811"),
              new Date()
            },
            new Object[] {
              5L,
              "GCP",
              "Dataflow",
              true,
              543.210,
              new BigDecimal("910365731946235.5322603719"),
              new Date()
            },
            new Object[] {6L, "Nulls", null, null, null, null, null});

    ExecutionDataSetMeta setMeta =
        new ExecutionDataSetMeta(
            "firstRows", "12345-logchannel-id", "transformName", "0", "First rows of transform");
    ExecutionData data =
        ExecutionDataBuilder.of()
            .addDataSets(Map.of("firstRows", new RowBuffer(rowMeta, rows)))
            .addSetMeta(Map.of("firstRows", setMeta))
            .withParentId("parentId")
            .withOwnerId("ownerId")
            .build();

    // Serialize to JSON and back
    //
    ObjectMapper objectMapper = HopJson.newMapper();
    String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);

    assertNotNull(json);

    ExecutionData copy = objectMapper.readValue(json, ExecutionData.class);

    assertNotNull(copy);

    assertEquals(data, copy);
  }
}
