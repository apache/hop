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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.injection.inheritance.MetaBeanChild;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class MetaAnnotationInjectionTest {
  private static final String FIELD_ONE = "FIELD_ONE";
  private static final String COMPLEX_NAME = "COMPLEX_NAME";
  private static final String TEST_NAME = "TEST_NAME";
  private IHopMetadataProvider metadataProvider;

  @BeforeEach
  void before() {
    HopLogStore.init();
    metadataProvider = new MemoryMetadataProvider();
  }

  @Test
  void testInjectionDescription() {
    BeanInjectionInfo<MetaBeanLevel1> ri = new BeanInjectionInfo<>(MetaBeanLevel1.class);

    assertEquals(3, ri.getGroups().size());
    assertEquals("", ri.getGroups().get(0).getKey());
    assertEquals("FILENAME_LINES", ri.getGroups().get(1).getKey());
    assertEquals("FILENAME_LINES2", ri.getGroups().get(2).getKey());

    assertTrue(ri.getProperties().containsKey("SEPARATOR"));
    assertTrue(ri.getProperties().containsKey("FILENAME"));
    assertTrue(ri.getProperties().containsKey("BASE"));
    assertTrue(ri.getProperties().containsKey("FIRST"));

    assertEquals("FILENAME_LINES", ri.getProperties().get("FILENAME").getGroupKey());
    assertEquals("!DESCRIPTION!", ri.getDescription("DESCRIPTION"));
  }

  @Test
  void testInjectionSets() throws Exception {
    MetaBeanLevel1 obj = new MetaBeanLevel1();

    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString("f1"));
    meta.addValueMeta(new ValueMetaString("f2"));
    meta.addValueMeta(new ValueMetaString("fstrint"));
    meta.addValueMeta(new ValueMetaString("fstrlong"));
    //  STLOCALE
    meta.addValueMeta(new ValueMetaString("fstrboolean"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(meta, "<sep>", "/tmp/file.txt", "123", "1234567891213", "y"));
    rows.add(new RowMetaAndData(meta, "<sep>", "/tmp/file2.txt", "123", "1234567891213", "y"));

    BeanInjector<?> inj = buildBeanInjectorFor(MetaBeanLevel1.class, metadataProvider);
    inj.setProperty(obj, "SEPARATOR", rows, "f1");
    inj.setProperty(obj, "FILENAME", rows, "f2");
    inj.setProperty(obj, "FILENAME_ARRAY", rows, "f2");
    inj.setProperty(obj, "FBOOLEAN", rows, "fstrboolean");
    inj.setProperty(obj, "FINT", rows, "fstrint");
    inj.setProperty(obj, "FLONG", rows, "fstrlong");
    inj.setProperty(obj, "FIRST", rows, "fstrint");

    assertEquals("<sep>", obj.getSub().getSeparator());
    assertEquals("/tmp/file.txt", obj.getSub().getFiles()[0].getName());
    assertTrue(obj.fboolean);
    assertEquals(123, obj.fint);
    assertEquals(1234567891213L, obj.flong);
    assertEquals("123", obj.getSub().first());
    assertArrayEquals(
        new String[] {"/tmp/file.txt", "/tmp/file2.txt"}, obj.getSub().getFilenames());
  }

  @Test
  void testInjectionConstant() throws Exception {
    MetaBeanLevel1 obj = new MetaBeanLevel1();

    BeanInjector<?> inj = buildBeanInjectorFor(MetaBeanLevel1.class, metadataProvider);
    inj.setProperty(obj, "SEPARATOR", null, "<sep>");
    inj.setProperty(obj, "FINT", null, "123");
    inj.setProperty(obj, "FLONG", null, "1234567891213");
    inj.setProperty(obj, "FBOOLEAN", null, "true");
    inj.setProperty(obj, "FILENAME", null, "f1");
    inj.setProperty(obj, "FILENAME_ARRAY", null, "f2");

    assertEquals("<sep>", obj.getSub().getSeparator());
    assertTrue(obj.fboolean);
    assertEquals(123, obj.fint);
    assertEquals(1234567891213L, obj.flong);
    assertNull(obj.getSub().getFiles());
    assertEquals("f2", obj.getSub().getFilenames()[0]);

    obj.getSub().files = new MetaBeanLevel3[] {new MetaBeanLevel3(), new MetaBeanLevel3()};
    obj.getSub().filenames = new String[] {"", "", ""};
    inj.setProperty(obj, "FILENAME", null, "f1");
    inj.setProperty(obj, "FILENAME_ARRAY", null, "f2");
    assertEquals(2, obj.getSub().getFiles().length);
    assertEquals("f1", obj.getSub().getFiles()[0].getName());
    assertEquals("f1", obj.getSub().getFiles()[1].getName());
    assertArrayEquals(new String[] {null, null, null, "f2"}, obj.getSub().getFilenames());
  }

  @Test
  void testInjectionForArrayPropertyWithoutDefaultConstructor_class_parameter()
      throws HopException {
    BeanInjector<?> beanInjector = buildBeanInjectorFor(MetadataBean.class, metadataProvider);
    MetadataBean targetBean = new MetadataBean();
    beanInjector.setProperty(targetBean, COMPLEX_NAME, createRowMetaAndData(), FIELD_ONE);

    assertNotNull(targetBean.getComplexField());
    assertEquals(1, targetBean.getComplexField().length);
    assertEquals(TEST_NAME, targetBean.getComplexField()[0].getFieldName());
  }

  @Test
  void testInjectionForArrayPropertyWithoutDefaultConstructorInterface_parameter()
      throws HopException {
    BeanInjector<?> beanInjector =
        buildBeanInjectorFor(MetadataBeanImplementsInterface.class, metadataProvider);
    MetadataBeanImplementsInterface targetBean = new MetadataBeanImplementsInterface();
    beanInjector.setProperty(targetBean, COMPLEX_NAME, createRowMetaAndData(), FIELD_ONE);

    assertNotNull(targetBean.getComplexField());
    assertEquals(1, targetBean.getComplexField().length);
    assertEquals(TEST_NAME, targetBean.getComplexField()[0].getFieldName());
  }

  @Test
  void testWrongDeclarations() {
    try {
      new BeanInjectionInfo<>(MetaBeanWrong1.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong2.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong3.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong4.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong5.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong6.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
    try {
      new BeanInjectionInfo<>(MetaBeanWrong7.class);
      fail();
    } catch (Exception ex) {
      // ignore
    }
  }

  @Test
  void testGenerics() {
    BeanInjectionInfo<MetaBeanChild> ri = new BeanInjectionInfo<>(MetaBeanChild.class);

    assertEquals(7, ri.getProperties().size());
    assertTrue(ri.getProperties().containsKey("BASE_ITEM_NAME"));
    assertTrue(ri.getProperties().containsKey("ITEM_CHILD_NAME"));
    assertTrue(ri.getProperties().containsKey("A"));
    assertTrue(ri.getProperties().containsKey("ITEM.BASE_ITEM_NAME"));
    assertTrue(ri.getProperties().containsKey("ITEM.ITEM_CHILD_NAME"));
    assertTrue(ri.getProperties().containsKey("SUB.BASE_ITEM_NAME"));
    assertTrue(ri.getProperties().containsKey("SUB.ITEM_CHILD_NAME"));

    assertEquals(String.class, ri.getProperties().get("A").getPropertyClass());
  }

  private static BeanInjector<?> buildBeanInjectorFor(
      Class<?> clazz, IHopMetadataProvider metadataProvider) {
    BeanInjectionInfo<?> metaBeanInfo = new BeanInjectionInfo<>(clazz);
    return new BeanInjector<>(metaBeanInfo, metadataProvider);
  }

  private static List<RowMetaAndData> createRowMetaAndData() {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString(FIELD_ONE));
    return Collections.singletonList(new RowMetaAndData(meta, TEST_NAME));
  }

  private interface MetadataInterface {}

  @Getter
  @Setter
  @InjectionSupported(localizationPrefix = "", groups = "COMPLEX")
  public static class MetadataBean {

    @InjectionDeep private ComplexField[] complexField;
  }

  @Getter
  @Setter
  public static class ComplexField {

    @Injection(name = "COMPLEX_NAME", group = "COMPLEX")
    private String fieldName;

    private final MetadataBean parentMeta;

    public ComplexField(MetadataBean parentMeta) {
      this.parentMeta = parentMeta;
    }
  }

  @Getter
  @Setter
  @InjectionSupported(localizationPrefix = "", groups = "COMPLEX")
  public static class MetadataBeanImplementsInterface implements MetadataInterface {

    @InjectionDeep private ComplexFieldWithInterfaceArg[] complexField;
  }

  @Getter
  public static class ComplexFieldWithInterfaceArg {

    @Setter
    @Injection(name = "COMPLEX_NAME", group = "COMPLEX")
    private String fieldName;

    private final MetadataInterface parentMeta;

    public ComplexFieldWithInterfaceArg(MetadataInterface parentMeta) {
      this.parentMeta = parentMeta;
    }
  }
}
