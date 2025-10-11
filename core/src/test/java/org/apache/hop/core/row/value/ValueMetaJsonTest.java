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

package org.apache.hop.core.row.value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.IValueMeta;
import org.junit.Test;

public class ValueMetaJsonTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static JsonNode obj(String json) throws Exception {
    return mapper.readTree(json);
  }

  // -------------------------
  // Conversions
  // -------------------------

  @Test
  public void testConvertPrettyPrinting() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    ObjectNode o = mapper.createObjectNode().put("a", 1).put("b", "x");

    vm.setPrettyPrinting(false);
    String compact = vm.getString(o);
    assertEquals("{\"a\":1,\"b\":\"x\"}", compact);

    vm.setPrettyPrinting(true);
    String pretty = vm.getString(o);
    assertTrue(pretty.contains("\n"));
    assertTrue(pretty.contains("  \"a\""));
  }

  @Test
  public void testCloneValueData() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    ObjectNode o = mapper.createObjectNode().put("a", 1);
    JsonNode cloned = (JsonNode) vm.cloneValueData(o);
    assertNotSame(o, cloned);
    assertEquals(o, cloned);
  }

  @Test
  public void testGetJsonAcrossStorageTypes() throws Exception {
    // NORMAL with JsonNode
    ValueMetaJson normal = new ValueMetaJson("j");
    normal.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    ObjectNode o = mapper.createObjectNode().put("k", "v");
    assertSame(o, normal.getJson(o));

    // NORMAL with Map
    var mapped = normal.getJson(java.util.Map.of("a", 1));
    assertEquals(obj("{\"a\":1}"), mapped);

    // BINARY_STRING
    ValueMetaJson bin = new ValueMetaJson("j");
    bin.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    byte[] bytes = "{\"x\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    assertEquals(obj("{\"x\":true}"), bin.getJson(bytes));

    // INDEXED
    ValueMetaJson idx = new ValueMetaJson("j");
    idx.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    JsonNode v0 = obj("{\"i\":0}");
    JsonNode v1 = obj("{\"i\":1}");
    idx.setIndex(new Object[] {v0, v1});
    assertEquals(v1, idx.getJson(1));
  }

  // -------------------------
  // Comparison
  // -------------------------

  @Test
  public void testJsonCompare() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");

    // Kind precedence
    JsonNode NULL = NullNode.getInstance();
    JsonNode MISSING = MissingNode.getInstance();
    JsonNode BIN = BinaryNode.valueOf(new byte[] {0x01});
    JsonNode STR = TextNode.valueOf("a");
    JsonNode NUM = DecimalNode.valueOf(new BigDecimal("2.5"));
    JsonNode BOOL = BooleanNode.TRUE;
    JsonNode ARY = mapper.createArrayNode().add(1).add(2);
    JsonNode OBJ = mapper.createObjectNode().put("a", 1);

    assertTrue(vm.typeCompare(NULL, MISSING) < 0);
    assertTrue(vm.typeCompare(MISSING, BIN) < 0);
    assertTrue(vm.typeCompare(BIN, STR) < 0);
    assertTrue(vm.typeCompare(STR, NUM) < 0);
    assertTrue(vm.typeCompare(NUM, BOOL) < 0);
    assertTrue(vm.typeCompare(BOOL, ARY) < 0);
    assertTrue(vm.typeCompare(ARY, OBJ) < 0);

    // Same-kind
    assertTrue(vm.typeCompare(IntNode.valueOf(10), IntNode.valueOf(11)) < 0);

    // Arrays
    JsonNode a1 = obj("[1,2]");
    JsonNode a2 = obj("[1,3]");
    assertTrue(vm.typeCompare(a1, a2) < 0);

    // Objects
    JsonNode o1 = obj("{\"a\":1,\"b\":2}");
    JsonNode o2 = obj("{\"b\":2,\"a\":1}");
    assertEquals(0, vm.typeCompare(o1, o2));
    JsonNode o3 = obj("{\"a\":1,\"b\":3}");
    assertTrue(vm.typeCompare(o1, o3) < 0);
  }

  @Test
  public void testBinaryCompare() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    JsonNode a = BinaryNode.valueOf(new byte[] {(byte) 0xFF}); // 255
    JsonNode b = BinaryNode.valueOf(new byte[] {0x00}); // 0
    assertTrue(vm.typeCompare(a, b) > 0);

    JsonNode c = BinaryNode.valueOf(new byte[] {0x01, 0x02}); // longer one
    JsonNode d = BinaryNode.valueOf(new byte[] {0x01});
    assertTrue(vm.typeCompare(c, d) > 0);
  }

  // -------------------------
  // Read from ResultSet
  // -------------------------

  @Test
  public void testGetValueFromResultSet_BinaryStream() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    IDatabase idb = mock(IDatabase.class);
    ResultSet rs = mock(ResultSet.class);

    String json = "{\"x\":42}";
    InputStream in =
        new ByteArrayInputStream(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    when(rs.getBinaryStream(1)).thenReturn(in); // index+1 = 1

    Object out = vm.getValueFromResultSet(idb, rs, 0);
    assertTrue(out instanceof JsonNode);
    assertEquals(obj(json), out);
  }

  @Test
  public void testGetValueFromResultSet_FallbackObject() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    IDatabase idb = mock(IDatabase.class);
    ResultSet rs = mock(ResultSet.class);

    when(rs.getBinaryStream(1)).thenThrow(new SQLException("no binary"));
    when(rs.getObject(1)).thenReturn("{\"k\":\"v\"}");

    Object out = vm.getValueFromResultSet(idb, rs, 0);
    assertEquals(obj("{\"k\":\"v\"}"), out);
  }

  // -------------------------
  // Native type
  // -------------------------

  @Test
  public void testNativeTypeInfo() throws Exception {
    ValueMetaJson vm = new ValueMetaJson("j");
    JsonNode node = obj("{\"a\":1}");
    assertSame(node, vm.getNativeDataType(node));
    assertEquals(JsonNode.class, vm.getNativeDataTypeClass());
  }

  @Test
  public void testDatabaseColumnTypeDefinition() {
    ValueMetaJson vm = new ValueMetaJson("col");
    IDatabase pg = mock(IDatabase.class);
    when(pg.isPostgresVariant()).thenReturn(true);
    IDatabase other = mock(IDatabase.class);
    when(other.isPostgresVariant()).thenReturn(false);

    assertEquals("JSONB", vm.getDatabaseColumnTypeDefinition(pg, null, null, false, false, false));
    assertEquals(
        "col JSONB" + Const.CR,
        vm.getDatabaseColumnTypeDefinition(pg, null, null, false, true, true));

    assertEquals(
        "JSON", vm.getDatabaseColumnTypeDefinition(other, null, null, false, false, false));
    assertEquals(
        "col JSON" + Const.CR,
        vm.getDatabaseColumnTypeDefinition(other, null, null, false, true, true));
  }
}
