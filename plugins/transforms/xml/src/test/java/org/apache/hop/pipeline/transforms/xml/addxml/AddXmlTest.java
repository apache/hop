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
package org.apache.hop.pipeline.transforms.xml.addxml;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AddXmlTest {

  private TransformMockHelper<AddXmlMeta, AddXmlData> transformMockHelper;

  @Before
  public void setup() throws Exception {
    XmlField field = mock(XmlField.class);
    when(field.getElementName()).thenReturn("ADDXML_TEST");
    when(field.isAttribute()).thenReturn(true);

    transformMockHelper =
        new TransformMockHelper<>("ADDXML_TEST", AddXmlMeta.class, AddXmlData.class);
    Mockito.doReturn( transformMockHelper.iLogChannel ).when( transformMockHelper.logChannelFactory ).create( any(), any( ILoggingObject.class )  );

    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getOutputFields()).thenReturn(new XmlField[] {field});
    when(transformMockHelper.iTransformMeta.getRootNode()).thenReturn("ADDXML_TEST");
  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testProcessRow() throws HopException {
    AddXml addXML =
        new AddXml(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    addXML.init();
    addXML.setInputRowSets(asList(createSourceRowSet("ADDXML_TEST")));

    assertTrue(addXML.processRow());
    assertTrue(addXML.getErrors() == 0);
    assertTrue(addXML.getLinesWritten() > 0);
  }

  private IRowSet createSourceRowSet(String source) {
    IRowSet sourceRowSet = transformMockHelper.getMockInputRowSet(new String[] {source});
    IRowMeta sourceRowMeta = mock(IRowMeta.class);
    when(sourceRowMeta.getFieldNames()).thenReturn(new String[] {source});
    when(sourceRowSet.getRowMeta()).thenReturn(sourceRowMeta);

    return sourceRowSet;
  }
}
