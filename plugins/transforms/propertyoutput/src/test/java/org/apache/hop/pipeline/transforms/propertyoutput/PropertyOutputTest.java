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

package org.apache.hop.pipeline.transforms.propertyoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** A properties file takes neither a null key nor a null value. */
class PropertyOutputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @TempDir Path folder;

  private TransformMockHelper<PropertyOutputMeta, PropertyOutputData> helper;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "Properties output", PropertyOutputMeta.class, PropertyOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  @Test
  void aMissingValueIsWrittenEmpty() throws Exception {
    PropertyOutput transform = run(new Object[][] {{"a", "1"}, {"b", null}});

    assertEquals(0, transform.getErrors());
    Properties written = new Properties();
    try (InputStream in = HopVfs.getInputStream(folder.resolve("out.properties").toString())) {
      written.load(in);
    }
    assertEquals("1", written.getProperty("a"));
    assertEquals("", written.getProperty("b"));
  }

  @Test
  void aMissingKeyIsATransformError() throws Exception {
    PropertyOutput transform = run(new Object[][] {{null, "1"}});

    assertEquals(1, transform.getErrors());
  }

  private PropertyOutput run(Object[][] rows) throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("key"));
    inputRowMeta.addValueMeta(new ValueMetaString("value"));
    IRowSet inputRowSet = helper.getMockInputRowSet(rows);
    inputRowSet.setRowMeta(inputRowMeta);

    PropertyOutputMeta meta = new PropertyOutputMeta();
    meta.setKeyField("key");
    meta.setValueField("value");
    meta.getFileDetails().setFileName(folder.resolve("out").toString());
    meta.getFileDetails().setExtension("properties");

    PropertyOutput transform =
        new PropertyOutput(
            helper.transformMeta,
            meta,
            new PropertyOutputData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    transform.addRowSetToInputRowSets(inputRowSet);
    transform.setInputRowMeta(inputRowMeta);
    transform.init();

    boolean more = true;
    for (int call = 0; more && call <= rows.length; call++) {
      more = transform.processRow();
    }
    assertFalse(more, "the transform did not end");
    return transform;
  }
}
