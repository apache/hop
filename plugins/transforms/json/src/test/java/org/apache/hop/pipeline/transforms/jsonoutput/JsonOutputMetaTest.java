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

package org.apache.hop.pipeline.transforms.jsonoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

public class JsonOutputMetaTest {

  @Test
  void testLoadSave() throws Exception {
    JsonOutputMeta meta =
        TransformSerializationTestUtil.testSerialization("/json-output.xml", JsonOutputMeta.class);
    validate(meta);
  }

  private static void validate(JsonOutputMeta meta) {
    assertTrue(meta.isAddToResult());
    assertTrue(meta.isCreateParentFolder());
    assertFalse(meta.isDateInFilename());
    assertTrue(meta.isDoNotOpenNewFileInit());
    assertEquals("UTF-8", meta.getEncoding());
    assertEquals("json", meta.getExtension());
    assertFalse(meta.isFileAppended());
    assertFalse(meta.isFileAsCommand());
    assertEquals("filename", meta.getFileName());
    assertEquals("jsonBlock", meta.getJsonBloc());
    assertEquals("123", meta.getNrRowsInBloc());
    assertEquals("writetofile", meta.getOperationType());
    assertEquals("outputValue", meta.getOutputValue());
    assertFalse(meta.isPartNrInFilename());
    assertFalse(meta.isSpecifyingFormat());
    assertFalse(meta.isTimeInFilename());
    assertFalse(meta.isTransformNrInFilename());

    assertNotNull(meta.getOutputFields());
    assertEquals(3, meta.getOutputFields().size());
    JsonOutputField f1 = meta.getOutputFields().get(0);
    assertEquals("f1", f1.getFieldName());
    assertEquals("element1", f1.getElementName());

    JsonOutputField f2 = meta.getOutputFields().get(1);
    assertEquals("f2", f2.getFieldName());
    assertEquals("element2", f2.getElementName());

    JsonOutputField f3 = meta.getOutputFields().get(2);
    assertEquals("f3", f3.getFieldName());
    assertEquals("element3", f3.getElementName());
  }

  @Test
  void testGroupMappings() throws Exception {
    Map<String, Set<String>> map = HopMetadataInjector.findInjectionGroupKeys(JsonOutputMeta.class);
    assertEquals(1, map.size());
    Set<String> fields = map.get("FIELDS");
    assertTrue(fields.contains("name"));
    assertTrue(fields.contains("element"));
  }
}
