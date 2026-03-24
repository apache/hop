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

package org.apache.hop.pipeline.transforms.textfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TextFileOutputMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  public static List<String> getMetaAttributes() {
    return Arrays.asList(
        "separator",
        "enclosure",
        "enclosure_forced",
        "enclosure_fix_disabled",
        "header",
        "footer",
        "format",
        "compression",
        "encoding",
        "endedLine",
        "fileNameInField",
        "fileNameField",
        "create_parent_folder",
        "fileName",
        "servlet_output",
        "do_not_open_new_file_init",
        "extention",
        "append",
        "split",
        "haspartno",
        "add_date",
        "add_time",
        "SpecifyFormat",
        "date_time_format",
        "add_to_result_filenames",
        "pad",
        "fast_dump",
        "splitevery",
        "OutputFields",
        "schemaDefinition",
        "ignoreFields");
  }

  public static Map<String, String> getGetterMap() {
    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("separator", "getSeparator");
    getterMap.put("enclosure", "getEnclosure");
    getterMap.put("enclosure_forced", "isEnclosureForced");
    getterMap.put("enclosure_fix_disabled", "isEnclosureFixDisabled");
    getterMap.put("header", "isHeaderEnabled");
    getterMap.put("footer", "isFooterEnabled");
    getterMap.put("format", "getFileFormat");
    getterMap.put("compression", "getFileCompression");
    getterMap.put("encoding", "getEncoding");
    getterMap.put("endedLine", "getEndedLine");
    getterMap.put("fileNameInField", "isFileNameInField");
    getterMap.put("fileNameField", "getFileNameField");
    getterMap.put("create_parent_folder", "isCreateParentFolder");
    getterMap.put("fileName", "getFileName");
    getterMap.put("servlet_output", "isServletOutput");
    getterMap.put("do_not_open_new_file_init", "isDoNotOpenNewFileInit");
    getterMap.put("extention", "getExtension");
    getterMap.put("append", "isFileAppended");
    getterMap.put("split", "isTransformNrInFilename");
    getterMap.put("haspartno", "isPartNrInFilename");
    getterMap.put("add_date", "isDateInFilename");
    getterMap.put("add_time", "isTimeInFilename");
    getterMap.put("SpecifyFormat", "isSpecifyingFormat");
    getterMap.put("date_time_format", "getDateTimeFormat");
    getterMap.put("add_to_result_filenames", "isAddToResultFiles");
    getterMap.put("pad", "isPadded");
    getterMap.put("fast_dump", "isFastDump");
    getterMap.put("splitevery", "getSplitEvery");
    getterMap.put("OutputFields", "getOutputFields");
    getterMap.put("schemaDefinition", "getSchemaDefinition");
    getterMap.put("ignoreFields", "isIgnoreFields");
    return getterMap;
  }

  public static Map<String, String> getSetterMap() {
    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("separator", "setSeparator");
    setterMap.put("enclosure", "setEnclosure");
    setterMap.put("enclosure_forced", "setEnclosureForced");
    setterMap.put("enclosure_fix_disabled", "setEnclosureFixDisabled");
    setterMap.put("header", "setHeaderEnabled");
    setterMap.put("footer", "setFooterEnabled");
    setterMap.put("format", "setFileFormat");
    setterMap.put("compression", "setFileCompression");
    setterMap.put("encoding", "setEncoding");
    setterMap.put("endedLine", "setEndedLine");
    setterMap.put("fileNameInField", "setFileNameInField");
    setterMap.put("fileNameField", "setFileNameField");
    setterMap.put("create_parent_folder", "setCreateParentFolder");
    setterMap.put("fileName", "setFileName");
    setterMap.put("servlet_output", "setServletOutput");
    setterMap.put("do_not_open_new_file_init", "setDoNotOpenNewFileInit");
    setterMap.put("extention", "setExtension");
    setterMap.put("append", "setFileAppended");
    setterMap.put("split", "setTransformNrInFilename");
    setterMap.put("haspartno", "setPartNrInFilename");
    setterMap.put("add_date", "setDateInFilename");
    setterMap.put("add_time", "setTimeInFilename");
    setterMap.put("SpecifyFormat", "setSpecifyingFormat");
    setterMap.put("date_time_format", "setDateTimeFormat");
    setterMap.put("add_to_result_filenames", "setAddToResultFiles");
    setterMap.put("pad", "setPadded");
    setterMap.put("fast_dump", "setFastDump");
    setterMap.put("splitevery", "setSplitEvery");
    setterMap.put("OutputFields", "setOutputFields");
    setterMap.put("schemaDefinition", "setSchemaDefinition");
    setterMap.put("ignoreFields", "setIgnoreFields");
    return setterMap;
  }

  public static Map<String, IFieldLoadSaveValidator<?>> getAttributeValidators() {
    return new HashMap<>();
  }

  public static Map<String, IFieldLoadSaveValidator<?>> getTypeValidators() {
    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();
    typeValidators.put(
        TextFileField[].class.getCanonicalName(),
        new ArrayLoadSaveValidator<>(new TextFileFieldLoadSaveValidator()));
    return typeValidators;
  }

  @Test
  void testLoadSave() throws Exception {
    Path path =
        Paths.get(Objects.requireNonNull(getClass().getResource("/text-file-output.xml")).toURI());
    String xml = Files.readString(path);
    TextFileOutputMeta meta = new TextFileOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        TextFileOutputMeta.class,
        meta,
        new MemoryMetadataProvider());

    validate(meta);

    // Do a round trip:
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TextFileOutputMeta metaCopy = new TextFileOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        TextFileOutputMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validate(metaCopy);
  }

  private static void validate(TextFileOutputMeta meta) {
    assertEquals("schemaDefinition", meta.getSchemaDefinition());
    assertTrue(meta.isIgnoreFields());
    assertEquals(";", meta.getSeparator());
    assertEquals("\"", meta.getEnclosure());
    assertTrue(meta.isEnclosureForced());
    assertFalse(meta.isEnclosureFixDisabled());
    assertTrue(meta.isHeaderEnabled());
    assertTrue(meta.isFooterEnabled());
    assertEquals("UNIX", meta.getFileFormat());
    assertEquals("Snappy", meta.getFileCompression());
    assertEquals("UTF-8", meta.getEncoding());
    assertTrue(StringUtils.isEmpty(meta.getEndedLine()));
    assertTrue(meta.isFileNameInField());
    assertEquals("filenameField", meta.getFileNameField());
    assertTrue(meta.isCreateParentFolder());

    assertNotNull(meta.getFileSettings());
    assertEquals("filename.txt", meta.getFileSettings().getFileName());
    assertFalse(meta.getFileSettings().isServletOutput());
    assertTrue(meta.getFileSettings().isDoNotOpenNewFileInit());
    assertEquals("txt", meta.getFileSettings().getExtension());
    assertTrue(meta.getFileSettings().isFileAppended());
    assertFalse(meta.getFileSettings().isTransformNrInFilename());
    assertFalse(meta.getFileSettings().isPartNrInFilename());
    assertFalse(meta.getFileSettings().isDateInFilename());
    assertFalse(meta.getFileSettings().isTimeInFilename());
    assertFalse(meta.getFileSettings().isSpecifyingFormat());
    assertTrue(StringUtils.isEmpty(meta.getFileSettings().getDateTimeFormat()));
    assertTrue(meta.getFileSettings().isAddToResultFiles());
    assertFalse(meta.getFileSettings().isPadded());
    assertTrue(meta.getFileSettings().isFastDump());
    assertEquals("0", meta.getFileSettings().getSplitEveryRows());

    assertNotNull(meta.getOutputFields());
    assertEquals(3, meta.getOutputFields().size());
    TextFileField f1 = meta.getOutputFields().get(0);
    assertEquals("f1", f1.getName());
    assertEquals(IValueMeta.TYPE_STRING, f1.getType());
    assertEquals(100, f1.getLength());
    assertEquals(IValueMeta.TRIM_TYPE_NONE, f1.getTrimType());
    assertEquals("half_even", f1.getRoundingType());

    TextFileField f2 = meta.getOutputFields().get(1);
    assertEquals("f2", f2.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f2.getType());
    assertEquals(7, f2.getLength());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, f2.getTrimType());
    assertEquals("half_even", f2.getRoundingType());

    TextFileField f3 = meta.getOutputFields().get(2);
    assertEquals("f3", f3.getName());
    assertEquals(IValueMeta.TYPE_DATE, f3.getType());
    assertEquals(-1, f3.getLength());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, f3.getTrimType());
    assertEquals("half_even", f3.getRoundingType());
  }

  @Test
  void testVarReplaceSplit() {
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setDefault();
    meta.getFileSettings().setSplitEveryRows("${splitVar}");
    IVariables varSpace = new Variables();
    assertEquals(0, meta.getSplitEvery(varSpace));
    String fileName =
        meta.buildFilename("foo", "txt2", varSpace, 0, null, 3, false, null, 0, false, meta);
    assertEquals("foo.txt2", fileName);
    varSpace.setVariable("splitVar", "2");
    assertEquals(2, meta.getSplitEvery(varSpace));
    fileName = meta.buildFilename("foo", "txt2", varSpace, 0, null, 5, false, null, 0, false, meta);
    assertEquals("foo_5.txt2", fileName);
  }

  public static class TextFileFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<TextFileField> {
    Random rand = new Random();

    @Override
    public TextFileField getTestObject() {
      String name = UUID.randomUUID().toString();
      int type =
          ValueMetaFactory.getIdForValueMeta(
              ValueMetaFactory.getValueMetaNames()[
                  rand.nextInt(ValueMetaFactory.getValueMetaNames().length)]);
      String format = UUID.randomUUID().toString();
      int length = Math.abs(rand.nextInt());
      int precision = Math.abs(rand.nextInt());
      String currencySymbol = UUID.randomUUID().toString();
      String decimalSymbol = UUID.randomUUID().toString();
      String groupSymbol = UUID.randomUUID().toString();
      String nullString = UUID.randomUUID().toString();

      return new TextFileField(
          name,
          type,
          format,
          length,
          precision,
          currencySymbol,
          decimalSymbol,
          groupSymbol,
          nullString,
          "floor");
    }

    @Override
    public boolean validateTestObject(TextFileField testObject, Object actual) {
      if (!(actual instanceof TextFileField) || testObject.compare(actual) != 0) {
        return false;
      }
      TextFileField act = (TextFileField) actual;
      if (testObject.getName().equals(act.getName())
          && testObject.getType() == act.getType()
          && testObject.getFormat().equals(act.getFormat())
          && testObject.getLength() == act.getLength()
          && testObject.getPrecision() == act.getPrecision()
          && testObject.getCurrencySymbol().equals(act.getCurrencySymbol())
          && testObject.getDecimalSymbol().equals(act.getDecimalSymbol())
          && testObject.getGroupingSymbol().equals(act.getGroupingSymbol())
          && testObject.getNullString().equals(act.getNullString())) {
        return true;
      } else {
        return false;
      }
    }
  }
}
