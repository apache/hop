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

package org.apache.hop.pipeline.transforms.orabulkloader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * Blank log, bad and discard files must be left off the sqlldr command line. Up to 2.17 a saved
 * {@code <log_file/>} loaded as null; since 2.18 it loads as an empty string, which must not turn
 * into {@code log='<working directory>'}.
 */
class OraBulkLoaderBlankFilesCommandLineTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<OraBulkLoaderMeta, OraBulkLoaderData> helper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper =
        new TransformMockHelper<>(
            "Oracle bulk loader", OraBulkLoaderMeta.class, OraBulkLoaderData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getUsername()).thenReturn("scott");
    when(databaseMeta.getPassword()).thenReturn("tiger");
    when(databaseMeta.getURL(any(IVariables.class)))
        .thenReturn("jdbc:oracle:thin:@localhost:1521:XE");
    when(helper.pipelineMeta.findDatabase(eq("ora"), any(IVariables.class)))
        .thenReturn(databaseMeta);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void blankLogBadAndDiscardFilesAreOmitted() throws Exception {
    String commandLine = commandLine("<log_file/><bad_file/><discard_file/>");

    assertTrue(commandLine.contains(" control='"), commandLine);
    assertFalse(commandLine.contains(" log="), commandLine);
    assertFalse(commandLine.contains(" bad="), commandLine);
    assertFalse(commandLine.contains(" discard="), commandLine);
  }

  @Test
  void specifiedLogBadAndDiscardFilesArePassed() throws Exception {
    String commandLine =
        commandLine(
            "<log_file>/tmp/ora/load.log</log_file>"
                + "<bad_file>/tmp/ora/load.bad</bad_file>"
                + "<discard_file>/tmp/ora/load.dsc</discard_file>");

    assertTrue(commandLine.matches(".* log='[^']*load\\.log'.*"), commandLine);
    assertTrue(commandLine.matches(".* bad='[^']*load\\.bad'.*"), commandLine);
    assertTrue(commandLine.matches(".* discard='[^']*load\\.dsc'.*"), commandLine);
  }

  private String commandLine(String fileTags) throws Exception {
    String xml =
        "<transform>"
            + "<connection>ora</connection>"
            + "<sqlldr>/opt/oracle/bin/sqlldr</sqlldr>"
            + "<control_file>/tmp/ora/load.ctl</control_file>"
            + "<data_file>/tmp/ora/load.dat</data_file>"
            + "<load_method>AUTO_END</load_method>"
            + fileTags
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    OraBulkLoaderMeta meta = new OraBulkLoaderMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());

    OraBulkLoader loader =
        new OraBulkLoader(
            helper.transformMeta,
            meta,
            new OraBulkLoaderData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    return loader.createCommandLine(meta, false);
  }
}
