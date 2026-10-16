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

package org.apache.hop.workflow.actions.xml.dtdvalidator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Check must validate bean properties, not the filename strings stored in them. */
class DtdValidatorCheckTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void variablePathIsNotUsedAsPropertyName() throws Exception {
    Path files = tempDir.resolve("files");
    Files.createDirectories(files);
    Files.writeString(files.resolve("dummy.xml"), "<a/>", StandardCharsets.UTF_8);
    Files.writeString(files.resolve("dummy.dtd"), "<!ELEMENT a EMPTY>", StandardCharsets.UTF_8);

    DtdValidator action = new DtdValidator("DTD validator");
    action.setXmlFilename("${Internal.Entry.Current.Directory}/files/dummy.xml");
    action.setDtdFilename("${Internal.Entry.Current.Directory}/files/dummy.dtd");
    action.setDtdIntern("N");

    Variables variables = new Variables();
    variables.setVariable("Internal.Entry.Current.Directory", tempDir.toAbsolutePath().toString());

    List<String> errors = errorTexts(action, variables);

    assertTrue(errors.isEmpty(), "Filename must not be treated as a bean property: " + errors);
  }

  @Test
  void internalDtdDoesNotRequireExternalDtdFile() throws Exception {
    Path xml = tempDir.resolve("files").resolve("dummy.xml");
    Files.createDirectories(xml.getParent());
    Files.writeString(xml, "<a/>", StandardCharsets.UTF_8);

    DtdValidator action = new DtdValidator("DTD validator");
    action.setXmlFilename(xml.toAbsolutePath().toString());
    action.setDtdFilename(null);
    action.setDtdIntern("Y");

    List<String> errors = errorTexts(action, new Variables());

    assertTrue(errors.isEmpty(), "Internal DTD must not require dtdFilename: " + errors);
  }

  @Test
  void externalDtdBlankReportsTheBeanProperty() throws Exception {
    Path xml = tempDir.resolve("files").resolve("dummy.xml");
    Files.createDirectories(xml.getParent());
    Files.writeString(xml, "<a/>", StandardCharsets.UTF_8);

    DtdValidator action = new DtdValidator("DTD validator");
    action.setXmlFilename(xml.toAbsolutePath().toString());
    action.setDtdFilename(null);
    action.setDtdIntern("N");

    List<String> errors = errorTexts(action, new Variables());

    assertTrue(
        errors.stream().anyMatch(text -> text.contains("dtdFilename")),
        "Blank external DTD must fail on the bean property, got: " + errors);
    assertFalse(
        errors.stream().anyMatch(text -> text.contains("dummy.xml")),
        "XML path must not be used as a property name, got: " + errors);
  }

  private static List<String> errorTexts(DtdValidator action, Variables variables) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, new WorkflowMeta(), variables, new MemoryMetadataProvider());
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
