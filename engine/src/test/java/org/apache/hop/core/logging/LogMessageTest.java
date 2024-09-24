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

package org.apache.hop.core.logging;

import static org.junit.Assert.assertEquals;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class LogMessageTest {
  private LogMessage logMessage;

  private static final String LOG_MESSAGE = "Test Message";
  private static final LogLevel LOG_LEVEL = LogLevel.BASIC;
  private static String treeLogChannelId;
  private static String simpleLogChannelId;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() {
    treeLogChannelId = LoggingRegistry.getInstance().registerLoggingSource(getTreeLoggingObject());
  }

  @After
  public void tearDown() {
    LoggingRegistry.getInstance().removeIncludingChildren(treeLogChannelId);
    System.clearProperty(Const.HOP_LOG_MARK_MAPPINGS);
  }

  @Test
  public void testWhenLogMarkMappingTurnOn_DetailedSubjectUsed() {
    turnOnLogMarkMapping();

    logMessage = new LogMessage(LOG_MESSAGE, treeLogChannelId, LOG_LEVEL);
    assertEquals(LOG_MESSAGE, logMessage.getMessage());
    assertEquals(LOG_LEVEL, logMessage.getLevel());
    assertEquals(treeLogChannelId, logMessage.getLogChannelId());
    assertEquals(
        "[PIPELINE_SUBJECT].[TRANSFORM_SUBJECT].PIPELINE_CHILD_SUBJECT", logMessage.getSubject());
  }

  @Test
  public void testWhenLogMarkMappingTurnOff_SimpleSubjectUsed() {
    turnOffLogMarkMapping();

    logMessage = new LogMessage(LOG_MESSAGE, treeLogChannelId, LOG_LEVEL);
    assertEquals(LOG_MESSAGE, logMessage.getMessage());
    assertEquals(LOG_LEVEL, logMessage.getLevel());
    assertEquals(treeLogChannelId, logMessage.getLogChannelId());
    assertEquals("PIPELINE_CHILD_SUBJECT", logMessage.getSubject());
  }

  @Test
  public void
      testWhenLogMarkMappingTurnOnAndNoSubMappingUsed_DetailedSubjectContainsOnlySimpleSubject() {
    turnOnLogMarkMapping();

    simpleLogChannelId =
        LoggingRegistry.getInstance().registerLoggingSource(getLoggingObjectWithOneParent());

    logMessage = new LogMessage(LOG_MESSAGE, simpleLogChannelId, LOG_LEVEL);
    assertEquals(LOG_MESSAGE, logMessage.getMessage());
    assertEquals(LOG_LEVEL, logMessage.getLevel());
    assertEquals(simpleLogChannelId, logMessage.getLogChannelId());
    assertEquals("PIPELINE_SUBJECT", logMessage.getSubject());

    LoggingRegistry.getInstance().removeIncludingChildren(simpleLogChannelId);
  }

  @Test
  public void testToString() {
    turnOnLogMarkMapping();

    simpleLogChannelId =
        LoggingRegistry.getInstance().registerLoggingSource(getLoggingObjectWithOneParent());

    LogMessage msg = new LogMessage("Log message", simpleLogChannelId, LogLevel.DEBUG);

    assertEquals("PIPELINE_SUBJECT - Log message", msg.toString());
  }

  @Test
  public void testToString_withOneArgument() {
    turnOnLogMarkMapping();

    simpleLogChannelId =
        LoggingRegistry.getInstance().registerLoggingSource(getLoggingObjectWithOneParent());

    LogMessage msg =
        new LogMessage(
            "Log message for {0}", simpleLogChannelId, new String[] {"Test"}, LogLevel.DEBUG);

    assertEquals("PIPELINE_SUBJECT - Log message for Test", msg.toString());
  }

  @Test
  public void testGetMessage() {
    LogMessage msg =
        new LogMessage(
            "m {0}, {1}, {2}, {3}, {4,number,#.00}, {5} {foe}",
            "Channel 01", new Object[] {"Foo", "{abc}", "", null, 123}, LogLevel.DEBUG);
    assertEquals("m Foo, {abc}, , null, 123.00, {5} {foe}", msg.getMessage());
  }

  private void turnOnLogMarkMapping() {
    HopConfig.getInstance()
        .setDescribedVariable(new DescribedVariable(Const.HOP_LOG_MARK_MAPPINGS, "Y", ""));
  }

  private void turnOffLogMarkMapping() {
    HopConfig.getInstance()
        .setDescribedVariable(new DescribedVariable(Const.HOP_LOG_MARK_MAPPINGS, "N", ""));
  }

  private static ILoggingObject getTreeLoggingObject() {
    ILoggingObject rootLogObject =
        new SimpleLoggingObject("ROOT_SUBJECT", LoggingObjectType.HOP_GUI, null);
    ILoggingObject pipelineLogObject =
        new SimpleLoggingObject("PIPELINE_SUBJECT", LoggingObjectType.PIPELINE, rootLogObject);
    ILoggingObject transformLogObject =
        new SimpleLoggingObject(
            "TRANSFORM_SUBJECT", LoggingObjectType.TRANSFORM, pipelineLogObject);
    return new SimpleLoggingObject(
        "PIPELINE_CHILD_SUBJECT", LoggingObjectType.PIPELINE, transformLogObject);
  }

  private static ILoggingObject getLoggingObjectWithOneParent() {
    ILoggingObject rootLogObject =
        new SimpleLoggingObject("ROOT_SUBJECT", LoggingObjectType.HOP_GUI, null);

    return new SimpleLoggingObject("PIPELINE_SUBJECT", LoggingObjectType.PIPELINE, rootLogObject);
  }
}
