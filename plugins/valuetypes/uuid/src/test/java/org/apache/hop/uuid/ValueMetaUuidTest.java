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

package org.apache.hop.uuid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

public class ValueMetaUuidTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Before
  public void setupOnce() throws Exception {
    HopClientEnvironment.init();
  }

  private ValueMetaUuid vm(String name) {
    return new ValueMetaUuid(name);
  }

  @Test
  public void testUuidTypeDescription() throws Exception {
    int uuidId = ValueMetaFactory.getIdForValueMeta("UUID");
    assertEquals("UUID", IValueMeta.getTypeDescription(uuidId));
  }

  @Test
  public void testNativeClassIsUuid() {
    assertEquals(UUID.class, vm("id").getNativeDataTypeClass());
  }

  @Test
  public void testConvertToUuid() throws Exception {
    ValueMetaUuid dst = vm("id");
    // UUID is converted to UUID (kept the sa,e)
    UUID u = UUID.randomUUID();
    assertSame(u, dst.convertData(dst, u));

    // String gets converted to UUID
    IValueMeta src = new ValueMetaString("id");
    u = UUID.randomUUID();
    Object out = dst.convertData(src, u.toString());
    assertTrue(out instanceof UUID);
    assertEquals(u, out);

    // UUID conversion is storage type is indexed
    ValueMetaString indexSrc = new ValueMetaString("id");
    indexSrc.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    UUID u2 = UUID.randomUUID();
    indexSrc.setIndex(new Object[] {u.toString(), u2.toString()});
    out = dst.convertData(indexSrc, Integer.valueOf(1));
    assertTrue(out instanceof UUID);
    assertEquals(u2, out);

    // string with BINARY_STRING storage (lazy conversion)
    ValueMetaUuid binDst = vm("id");

    // source meta: String stored as bytes
    ValueMetaString binSrc = new ValueMetaString("id");
    binSrc.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);

    // target meta storageMetadata tells how to turn bytes into String
    ValueMetaString storage = new ValueMetaString("id");
    storage.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    storage.setStringEncoding("UTF-8");
    binDst.setStorageMetadata(storage);

    UUID u3 = UUID.randomUUID();
    byte[] lazyBytes = u3.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

    out = binDst.convertData(binSrc, lazyBytes);
    assertTrue(out instanceof UUID);
    assertEquals(u3, out);
  }

  @Test
  public void testConvertInvalidThrows() {
    ValueMetaUuid dst = vm("id");
    IValueMeta src = new ValueMetaString("id");
    try {
      dst.convertData(src, "not-a-uuid");
      fail("Expected HopValueException");
    } catch (HopValueException e) {
      // ok
    }
  }

  @Test
  public void testHashCode() throws Exception {
    ValueMetaUuid dst = vm("id");
    UUID u = UUID.randomUUID();
    int h1 = dst.hashCode(u);
    int h2 = dst.hashCode(u.toString());
    assertEquals(h1, h2);

    assertEquals(0, dst.hashCode(null));
  }

  @Test
  public void testCloneValueData() throws Exception {
    ValueMetaUuid dst = vm("id");
    UUID u = UUID.randomUUID();
    assertSame(u, dst.cloneValueData(u));
  }

  @Test
  public void testCompare() throws Exception {
    ValueMetaUuid dst = vm("id");
    UUID u = UUID.randomUUID();

    // equality across representations
    assertEquals(0, dst.compare(u, u.toString()));

    // ascending null handling
    dst.setSortedDescending(false);
    assertTrue(dst.compare(null, u) < 0);
    assertTrue(dst.compare(u, null) > 0);
    assertEquals(0, dst.compare(null, null));

    // descending null handling
    dst.setSortedDescending(true);
    assertTrue(dst.compare(null, u) > 0);
    assertTrue(dst.compare(u, null) < 0);
    assertEquals(0, dst.compare(null, null));

    UUID a = UUID.fromString("00000000-0000-0000-0000-000000000000");
    UUID b = UUID.fromString("00000000-0000-0000-0000-000000000001");
    // reset to ascending for ordering checks
    dst.setSortedDescending(false);
    assertTrue(dst.compare(a, b) < 0);
    assertTrue(dst.compare(b, a) > 0);
  }

  @Test
  public void testWriteDataNormalStorage() throws Exception {
    ValueMetaUuid dst = vm("id");
    UUID u = UUID.randomUUID();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);

    dst.writeData(dos, u);
    dos.flush();

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));

    // Because writeData() writes a null flag first,
    // true if the object is false
    assertFalse(dis.readBoolean());
    int len = dis.readInt();
    assertEquals(36, len);
    byte[] buf = new byte[len];
    dis.readFully(buf);
    assertEquals(u.toString(), new String(buf, StandardCharsets.UTF_8));
  }

  @Test
  public void testGetValueFromResultSetReadsString() throws Exception {
    ValueMetaUuid dst = vm("id");
    ResultSet rs = Mockito.mock(ResultSet.class);
    Mockito.when(rs.getObject(1)).thenReturn("123e4567-e89b-12d3-a456-426655440000");

    Object out = dst.getValueFromResultSet(Mockito.mock(IDatabase.class), rs, 0);
    assertTrue(out instanceof UUID);
    assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426655440000"), out);
  }

  @Test
  public void testGetValueFromResultSetReadsUuid() throws Exception {
    ValueMetaUuid dst = vm("id");
    ResultSet rs = Mockito.mock(ResultSet.class);
    UUID u = UUID.randomUUID();
    Mockito.when(rs.getObject(1)).thenReturn(u);

    Object out = dst.getValueFromResultSet(Mockito.mock(IDatabase.class), rs, 0);
    assertSame(u, out);
  }
}
