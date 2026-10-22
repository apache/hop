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

package org.apache.hop.workflow.actions.xml.xslt;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class XsltCheckTest {
  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void checkReadsTheLowerCaseFileNames() throws Exception {
    Path xml = Files.writeString(tempDir.resolve("source.xml"), "<a/>");
    Path xsl = Files.writeString(tempDir.resolve("style.xsl"), "<b/>");
    Xslt action = new Xslt();
    action.setXmlfilename(xml.toAbsolutePath().toString());
    action.setXslfilename(xsl.toAbsolutePath().toString());
    action.setOutputfilename(tempDir.resolve("out.xml").toAbsolutePath().toString());

    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, new Variables(), null);

    assertTrue(errors(remarks).isEmpty(), () -> errors(remarks).toString());
  }

  private static List<String> errors(List<ICheckResult> remarks) {
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
