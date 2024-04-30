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

package org.apache.hop.pipeline.transforms.excelinput;

import java.util.Arrays;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

/** Base class for all Fixed input transform tests. */
@Ignore("No tests in abstract base class")
public class BaseExcelParsingTest
    extends BaseParsingTest<ExcelInputMeta, ExcelInputData, ExcelInput> {
  /** Initialize transform info. */
  @Before
  public void before() {
    inPrefix = '/' + this.getClass().getPackage().getName().replace('.', '/') + "/files/";

    meta = new ExcelInputMeta();
    meta.setDefault();

    data = new ExcelInputData();
    data.outputRowMeta = new RowMeta();
  }

  @After
  public void after() {
    if (transform != null) {
      transform.dispose();
      transform = null;
    }
  }

  /** Initialize for processing specified file. */
  protected void init(String filename) throws Exception {
    ExcelInputMeta.EIFile file = new ExcelInputMeta.EIFile();
    file.setName(getFile(filename).getURL().getFile());
    file.setMask("");
    file.setExcludeMask("");
    file.setRequired("Y");
    file.setIncludeSubFolders("N");
    meta.getFiles().add(file);

    transform =
        new ExcelInput(transformMeta, meta, new ExcelInputData(), 1, pipelineMeta, pipeline);
    transform.init();
    transform.addRowListener(rowListener);
  }

  /** Declare fields for test. */
  protected void setFields(ExcelInputField... fields) throws Exception {
    meta.setFields(Arrays.asList(fields));
    meta.getFields(data.outputRowMeta, meta.getName(), null, null, new Variables(), null);
  }

  /** For BaseFileInput fields. */
  @Override
  protected void setFields(BaseFileField... fields) throws Exception {
    throw new RuntimeException("Not implemented");
  }
}
