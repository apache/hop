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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.errorhandling.AbstractFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.CompositeFileErrorHandler;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

/**
 * Blank error and line-number file folders must not register file error handlers. Up to 2.17 a
 * saved {@code <error_line_files_destination_directory/>} loaded as null; since 2.18 it loads as an
 * empty string, which would register a handler writing to {@code "" + "/" + name}, i.e. the
 * filesystem root.
 */
class ExcelInputBlankErrorFoldersTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ExcelInputMeta, ExcelInputData> helper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Excel input", ExcelInputMeta.class, ExcelInputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    doReturn(new Date()).when(helper.pipeline).getExecutionStartDate();
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void blankFoldersRegisterNoErrorHandlers() throws Exception {
    ExcelInputMeta meta =
        loadMeta(
            "<error_line_files_destination_directory/>"
                + "<line_number_files_destination_directory/>");

    List<String> directories = initAndGetHandlerDirectories(meta);

    assertTrue(
        directories.isEmpty(),
        "No error file handlers expected for blank folders, got directories " + directories);
  }

  @Test
  void specifiedFoldersRegisterErrorHandlers(@TempDir Path tempDir) throws Exception {
    String folder = tempDir.toString();
    ExcelInputMeta meta =
        loadMeta(
            "<error_line_files_destination_directory>"
                + folder
                + "</error_line_files_destination_directory>"
                + "<line_number_files_destination_directory>"
                + folder
                + "</line_number_files_destination_directory>");

    List<String> directories = initAndGetHandlerDirectories(meta);

    assertEquals(List.of(folder, folder), directories);
  }

  private List<String> initAndGetHandlerDirectories(ExcelInputMeta meta) throws Exception {
    ExcelInputData data = new ExcelInputData();
    ExcelInput transform =
        new ExcelInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    // The result of init() is irrelevant here: the error handlers are set up first.
    transform.init();

    Field handlersField = CompositeFileErrorHandler.class.getDeclaredField("handlers");
    handlersField.setAccessible(true);
    Field directoryField = AbstractFileErrorHandler.class.getDeclaredField("destinationDirectory");
    directoryField.setAccessible(true);

    List<String> directories = new ArrayList<>();
    @SuppressWarnings("unchecked")
    List<IFileErrorHandler> handlers =
        (List<IFileErrorHandler>) handlersField.get(data.errorHandler);
    for (IFileErrorHandler handler : handlers) {
      directories.add((String) directoryField.get(handler));
    }
    return directories;
  }

  private static ExcelInputMeta loadMeta(String folderTags) throws Exception {
    String xml =
        "<transform>"
            + "<error_ignored>Y</error_ignored>"
            + "<bad_line_files_destination_directory/>"
            + folderTags
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    ExcelInputMeta meta = new ExcelInputMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
