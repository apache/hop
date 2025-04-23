/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.vfs.azure;

import java.util.stream.Stream;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class AzureFileNameParserTest {

  private AzureFileNameParser parser;

  @BeforeAll
  static void init() {
    AzureConfig azureConfig = new AzureConfig();
    azureConfig.setAccount("hopsa");
    azureConfig.setKey("aGVsbG93b3JsZA==");
    MockedStatic<AzureConfigSingleton> azureConfigSingleton =
        Mockito.mockStatic(AzureConfigSingleton.class);
    azureConfigSingleton.when(AzureConfigSingleton::getConfig).thenReturn(azureConfig);
  }

  @BeforeEach
  void setup() {
    parser = new AzureFileNameParser();
  }

  //  public AzureFileNameParserTest(
  //      String inputUri,
  //      String expectedScheme,
  //      String expectedContainer,
  //      String expectedPathAfterContainer,
  //      FileType expectedType) {
  //    this.inputUri = inputUri;
  //    this.expectedScheme = expectedScheme;
  //    this.expectedContainer = expectedContainer;
  //    this.expectedPathAfterContainer = expectedPathAfterContainer;
  //    this.expectedType = expectedType;
  //  }

  static Stream<Arguments> azureUris() {
    return Stream.of(
        Arguments.of(
            "azfs://hopsa/container/folder1/parquet-test-delo2-azfs-00-0001.parquet",
            "azfs",
            "container",
            "/folder1/parquet-test-delo2-azfs-00-0001.parquet",
            FileType.FILE),
        Arguments.of(
            "azfs:/hopsa/container/folder1/", "azfs", "container", "/folder1", FileType.FOLDER),
        Arguments.of("azure://test/folder1/", "azure", "test", "/folder1", FileType.FOLDER),
        Arguments.of(
            "azure://mycontainer/folder1/parquet-test-delo2-azfs-00-0001.parquet",
            "azure",
            "mycontainer",
            "/folder1/parquet-test-delo2-azfs-00-0001.parquet",
            FileType.FILE),
        Arguments.of(
            "azfs://hopsa/delo/delo3-azfs-00-0001.parquet",
            "azfs",
            "delo",
            "/delo3-azfs-00-0001.parquet",
            FileType.FILE),
        Arguments.of(
            "azfs://hopsa/container/folder1/", "azfs", "container", "/folder1", FileType.FOLDER),
        Arguments.of("azfs://account/container/", "azfs", "container", "", FileType.FOLDER),
        Arguments.of(
            "azfs://otheraccount/container/myfile.txt",
            "azfs",
            "container",
            "/myfile.txt",
            FileType.FILE),
        Arguments.of(
            "azfs:///account1/container/myfile.txt",
            "azfs",
            "container",
            "/myfile.txt",
            FileType.FILE),
        Arguments.of(
            "azfs:///fake/container/path/to/resource/myfile.txt",
            "azfs",
            "container",
            "/path/to/resource/myfile.txt",
            FileType.FILE));
  }

  @ParameterizedTest
  @MethodSource("azureUris")
  void parseUri(
      String inputUri,
      String expectedScheme,
      String expectedContainer,
      String expectedPathAfterContainer,
      FileType expectedType)
      throws FileSystemException {
    VfsComponentContext context = Mockito.mock(VfsComponentContext.class);

    AzureFileName actual = (AzureFileName) parser.parseUri(context, null, inputUri);

    System.out.println(inputUri);
    System.out.println("Scheme: " + actual.getScheme());
    System.out.println("Container: " + actual.getContainer());
    System.out.println("Path: " + actual.getPath());
    System.out.println("--------------------------");

    Assertions.assertEquals(expectedScheme, actual.getScheme());
    Assertions.assertEquals(expectedContainer, actual.getContainer());
    Assertions.assertEquals(expectedPathAfterContainer, actual.getPathAfterContainer());
    Assertions.assertEquals(expectedType, actual.getType());
  }
}
