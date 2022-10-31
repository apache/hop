/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.injection;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.injection.bean.BeanLevelInfo;
import org.apache.hop.core.injection.metadata.PropBeanChild;
import org.apache.hop.core.injection.metadata.PropBeanGrandChild;
import org.apache.hop.core.injection.metadata.PropBeanListChild;
import org.apache.hop.core.injection.metadata.PropBeanParent;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetaPropInjectionTest {

  /**
   * Test to see that the annotations are picked up properly in various scenarios
   *
   * @throws Exception
   */
  @Test
  public void testHopMetadataPropertyInjectionInfo() throws Exception {
    BeanInjectionInfo<PropBeanParent> info = new BeanInjectionInfo<>(PropBeanParent.class);
    assertEquals(4, info.getGroups().size());
    assertEquals(11, info.getProperties().size());

    // String PropBeanParent.stringField
    //
    BeanInjectionInfo<PropBeanParent>.Property prop = info.getProperties().get("str");
    assertNotNull(prop);
    assertEquals(2, prop.getPath().size());
    BeanLevelInfo beanLevelInfo = prop.getPath().get(1);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertEquals("stringField", beanLevelInfo.field.getName());

    // int PropBeanParent.intField
    //
    prop = info.getProperties().get("int");
    assertNotNull(prop);
    assertEquals(2, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(int.class, beanLevelInfo.leafClass);
    assertEquals("intField", beanLevelInfo.field.getName());

    // long PropBeanParent.longField
    //
    prop = info.getProperties().get("long");
    assertNotNull(prop);
    assertEquals(2, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(long.class, beanLevelInfo.leafClass);
    assertEquals("longField", beanLevelInfo.field.getName());

    // boolean PropBeanParent.booleanField
    //
    prop = info.getProperties().get("boolean");
    assertNotNull(prop);
    assertEquals(2, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(boolean.class, beanLevelInfo.leafClass);
    assertEquals("booleanField", beanLevelInfo.field.getName());

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("child1");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(PropBeanChild.class, beanLevelInfo.leafClass);
    assertEquals("child", beanLevelInfo.field.getName());
    beanLevelInfo = prop.getPath().get(2);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertEquals("childField1", beanLevelInfo.field.getName());

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("child2");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(PropBeanChild.class, beanLevelInfo.leafClass);
    assertEquals("child", beanLevelInfo.field.getName());
    beanLevelInfo = prop.getPath().get(2);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertEquals("childField2", beanLevelInfo.field.getName());

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("f1");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(PropBeanListChild.class, beanLevelInfo.leafClass);
    assertEquals("children", beanLevelInfo.field.getName());
    beanLevelInfo = prop.getPath().get(2);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertEquals("f1", beanLevelInfo.field.getName());

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("f2");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(PropBeanListChild.class, beanLevelInfo.leafClass);
    assertEquals("children", beanLevelInfo.field.getName());
    beanLevelInfo = prop.getPath().get(2);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertEquals("f2", beanLevelInfo.field.getName());

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("string");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());
    beanLevelInfo = prop.getPath().get(1);
    assertEquals(List.class, beanLevelInfo.leafClass);
    assertEquals("strings", beanLevelInfo.field.getName());
    beanLevelInfo = prop.getPath().get(2);
    assertEquals(String.class, beanLevelInfo.leafClass);
    assertNull(beanLevelInfo.field);
    assertNull(beanLevelInfo.getter);
    assertNull(beanLevelInfo.setter);

    // PropBeanChild PropBeanParent.child
    //
    prop = info.getProperties().get("grand_child_name");
    assertNotNull(prop);
    assertEquals(4, prop.getPath().size());
    assertEquals(PropBeanParent.class, prop.getPath().get(0).leafClass);
    assertEquals(PropBeanChild.class, prop.getPath().get(1).leafClass);
    assertEquals(PropBeanGrandChild.class, prop.getPath().get(2).leafClass);
    assertEquals(String.class, prop.getPath().get(3).leafClass);

    prop = info.getProperties().get("grand_child_description");
    assertNotNull(prop);
    assertEquals(4, prop.getPath().size());
    assertEquals(PropBeanParent.class, prop.getPath().get(0).leafClass);
    assertEquals(PropBeanChild.class, prop.getPath().get(1).leafClass);
    assertEquals(PropBeanGrandChild.class, prop.getPath().get(2).leafClass);
    assertEquals(String.class, prop.getPath().get(3).leafClass);
  }

  @Test
  public void testHopMetadataPropertyInjection() throws Exception {
    // The object to inject into
    //
    PropBeanParent parent = new PropBeanParent();
    BeanInjectionInfo<PropBeanParent> info = new BeanInjectionInfo<>(PropBeanParent.class);

    // The metadata to inject...
    //
    RowMetaAndData parentMetadata = new RowMetaAndData();
    parentMetadata.addValue(new ValueMetaString("stringValue"), "someString");
    parentMetadata.addValue(new ValueMetaString("intValue"), "123");
    parentMetadata.addValue(new ValueMetaString("longValue"), "987654321");
    parentMetadata.addValue(new ValueMetaString("booleanValue"), "true");
    parentMetadata.addValue(new ValueMetaString("childValue1"), "cv1");
    parentMetadata.addValue(new ValueMetaString("childValue2"), "cv2");
    parentMetadata.addValue(new ValueMetaString("grandChildName"), "someName");
    parentMetadata.addValue(new ValueMetaString("grandChildDescription"), "someDescription");

    BeanInjector<PropBeanParent> injector = new BeanInjector<>(info, new MemoryMetadataProvider());

    injector.setProperty(parent, "str", Arrays.asList(parentMetadata), "stringValue");
    injector.setProperty(parent, "int", Arrays.asList(parentMetadata), "intValue");
    injector.setProperty(parent, "long", Arrays.asList(parentMetadata), "longValue");
    injector.setProperty(parent, "boolean", Arrays.asList(parentMetadata), "booleanValue");
    injector.setProperty(parent, "child1", Arrays.asList(parentMetadata), "childValue1");
    injector.setProperty(parent, "child2", Arrays.asList(parentMetadata), "childValue2");
    injector.setProperty(
        parent, "grand_child_name", Arrays.asList(parentMetadata), "grandChildName");
    injector.setProperty(
        parent, "grand_child_description", Arrays.asList(parentMetadata), "grandChildDescription");

    assertEquals(parent.getStringField(), "someString");
    assertEquals(parent.getIntField(), 123);
    assertEquals(parent.getLongField(), 987654321L);
    assertTrue(parent.isBooleanField());
    assertEquals(parent.getChild().getChildField1(), "cv1");
    assertEquals(parent.getChild().getChildField2(), "cv2");
    assertEquals("someName", parent.getChild().getGrandChild().getGrandChildName());
    assertEquals("someDescription", parent.getChild().getGrandChild().getGrandChildDescription());

    IRowMeta stringRowMeta = new RowMetaBuilder().addString("stringField").build();
    List<RowMetaAndData> stringsRows =
        Arrays.asList(
            new RowMetaAndData(stringRowMeta, "string1"),
            new RowMetaAndData(stringRowMeta, "string2"),
            new RowMetaAndData(stringRowMeta, "string3"));
    injector.setProperty(parent, "string", stringsRows, "stringField");

    assertEquals(3, parent.getStrings().size());
    assertEquals("string1", parent.getStrings().get(0));
    assertEquals("string2", parent.getStrings().get(1));
    assertEquals("string3", parent.getStrings().get(2));

    // Inject a number of children into a list
    //
    IRowMeta entriesRowMeta =
        new RowMetaBuilder().addString("fieldF1").addString("fieldF2").build();
    List<RowMetaAndData> entriesRows =
        Arrays.asList(
            new RowMetaAndData(entriesRowMeta, "f1_1", "f2_1"),
            new RowMetaAndData(entriesRowMeta, "f1_2", "f2_2"),
            new RowMetaAndData(entriesRowMeta, "f1_3", "f2_3"));
    injector.setProperty(parent, "f1", entriesRows, "fieldF1");
    injector.setProperty(parent, "f2", entriesRows, "fieldF2");

    assertEquals(3, parent.getChildren().size());
    assertEquals(new PropBeanListChild("f1_1", "f2_1"), parent.getChildren().get(0));
    assertEquals(new PropBeanListChild("f1_2", "f2_2"), parent.getChildren().get(1));
    assertEquals(new PropBeanListChild("f1_3", "f2_3"), parent.getChildren().get(2));
  }
}
