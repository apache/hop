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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetAddress;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Values without a Spark type of their own (JSON, an internet address, ...) travel as their text
 * and come back as the Hop value.
 */
class HopSparkRowConverterJsonTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void aJsonValueTravelsAsTextAndComesBackAsJson() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaJson json = new ValueMetaJson("doc");
    rowMeta.addValueMeta(json);
    assertEquals(DataTypes.StringType, HopSparkRowConverter.toDataType(json));

    JsonNode node = json.convertStringToJson("{\"name\":\"Goku\",\"power\":9001}");
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    Row sparkRow = codec.toSpark(new Object[] {node});
    assertTrue(sparkRow.get(0) instanceof String);

    Object back = codec.toHop(sparkRow)[0];
    assertTrue(back instanceof JsonNode);
    assertEquals(node, back);
    assertEquals(node, codec.handOff(new Object[] {node})[0]);
    assertNull(codec.toHop(org.apache.spark.sql.RowFactory.create((Object) null))[0]);
  }

  @Test
  void anInternetAddressComesBackAsAnInternetAddress() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInternetAddress("ip"));
    InetAddress address = InetAddress.getByName("192.168.1.10");
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    Row sparkRow = codec.toSpark(new Object[] {address});
    assertTrue(sparkRow.get(0) instanceof String);
    assertEquals(address, codec.toHop(sparkRow)[0]);
    assertEquals(address, codec.handOff(new Object[] {address})[0]);
  }
}
