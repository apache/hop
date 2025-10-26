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

package org.apache.hop.pipeline.transforms.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class SalesforceTransformTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<SalesforceTransformMeta, SalesforceTransformData> smh;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws HopException {
    smh =
        new TransformMockHelper<>(
            "Salesforce", SalesforceTransformMeta.class, SalesforceTransformData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  @Test
  void testErrorHandling() {
    SalesforceTransformMeta meta = mock(SalesforceTransformMeta.class, Mockito.CALLS_REAL_METHODS);
    assertFalse(meta.supportsErrorHandling());
  }

  @Test
  void testInitDispose() {
    SalesforceTransformMeta meta = mock(SalesforceTransformMeta.class, Mockito.CALLS_REAL_METHODS);
    SalesforceTransform transform =
        spy(
            new MockSalesforceTransform(
                smh.transformMeta, meta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline));

    /*
     * Salesforce Transform should fail if username and password are not set
     * We should not set a default account for all users
     */
    meta.setDefault();
    assertFalse(transform.init());

    meta.setDefault();
    meta.setTargetUrl(null);
    assertFalse(transform.init());

    meta.setDefault();
    meta.setUsername("anonymous");
    assertFalse(transform.init());

    meta.setDefault();
    meta.setUsername("anonymous");
    meta.setPassword("myPwd");
    meta.setModule(null);
    assertFalse(transform.init());

    /*
     * After setting username and password, we should have enough defaults to properly init
     */
    meta.setDefault();
    meta.setUsername("anonymous");
    meta.setPassword("myPwd");
    assertTrue(transform.init());

    // Dispose check
    assertNotNull(smh.iTransformData.connection);
    transform.dispose();
    assertNull(smh.iTransformData.connection);
  }

  class MockSalesforceTransform
      extends SalesforceTransform<SalesforceTransformMeta, SalesforceTransformData> {
    public MockSalesforceTransform(
        TransformMeta transformMeta,
        SalesforceTransformMeta meta,
        SalesforceTransformData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }
  }

  @Test
  void createIntObjectTest() throws HopValueException {
    SalesforceTransform transform =
        spy(
            new MockSalesforceTransform(
                smh.transformMeta,
                smh.iTransformMeta,
                smh.iTransformData,
                0,
                smh.pipelineMeta,
                smh.pipeline));
    IValueMeta valueMeta = Mockito.mock(IValueMeta.class);
    Mockito.when(valueMeta.getType()).thenReturn(IValueMeta.TYPE_INTEGER);
    Object value = transform.normalizeValue(valueMeta, 100L);
    assertTrue(value instanceof Integer);
  }

  @Test
  void createDateObjectTest() throws HopValueException, ParseException {
    SalesforceTransform transform =
        spy(
            new MockSalesforceTransform(
                smh.transformMeta,
                smh.iTransformMeta,
                smh.iTransformData,
                0,
                smh.pipelineMeta,
                smh.pipeline));
    IValueMeta valueMeta = Mockito.mock(IValueMeta.class);
    DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    Date date = dateFormat.parse("12-10-2017 15:10:25");
    Mockito.when(valueMeta.isDate()).thenReturn(true);
    Mockito.when(valueMeta.getDateFormatTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));
    Mockito.when(valueMeta.getDate(date)).thenReturn(date);
    Object value = transform.normalizeValue(valueMeta, date);
    assertTrue(value instanceof Calendar);
    DateFormat minutesDateFormat = new SimpleDateFormat("mm:ss");
    // check not missing minutes and seconds
    assertEquals(
        minutesDateFormat.format(date), minutesDateFormat.format(((Calendar) value).getTime()));
  }
}
