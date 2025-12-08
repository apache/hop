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

package org.apache.hop.pipeline.transforms.mongodbinput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoDbInputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<MongoDbInputMeta, MongoDbInputData> mockHelper;
  private MongoDbConnection connection;
  private SerializableMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() throws HopException {
    mockHelper =
        new TransformMockHelper<>("MongoDbInput", MongoDbInputMeta.class, MongoDbInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    // Setup metadata provider
    metadataProvider = new SerializableMetadataProvider();
    connection = new MongoDbConnection();
    connection.setName("test-connection");
    connection.setHostname("localhost");
    connection.setPort("27017");
    connection.setDbName("testdb");
    metadataProvider.getSerializer(MongoDbConnection.class).save(connection);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testConstructor() {
    MongoDbInput transform =
        new MongoDbInput(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    assertNotNull(transform);
    assertNotNull(transform.getTransformMeta());
    assertNotNull(transform.getMeta());
    assertNotNull(transform.getData());
  }

  @Test
  void testInitFailsWhenConnectionNotFound() throws HopException, MongoDbException {
    MongoDbInputMeta meta = new MongoDbInputMeta();
    meta.setConnectionName("non-existent-connection");
    meta.setCollection("test-collection");

    MongoDbInputData data = new MongoDbInputData();
    MongoDbInput transform = createTransform(meta, data);

    // init() catches exceptions and returns false instead of throwing
    assertFalse(transform.init());
  }

  @Test
  void testInitFailsWhenDatabaseNameIsEmpty() throws HopException, MongoDbException {
    MongoDbConnection conn = new MongoDbConnection();
    conn.setName("test-conn");
    conn.setHostname("localhost");
    conn.setDbName(""); // Empty database name
    metadataProvider.getSerializer(MongoDbConnection.class).save(conn);

    MongoDbInputMeta meta = new MongoDbInputMeta();
    meta.setConnectionName("test-conn");
    meta.setCollection("test-collection");

    MongoDbInputData data = new MongoDbInputData();
    MongoDbInput transform = createTransform(meta, data);

    // init() catches exceptions and returns false instead of throwing
    assertFalse(transform.init());
  }

  @Test
  void testInitFailsWhenCollectionIsEmpty() throws HopException, MongoDbException {
    MongoDbInputMeta meta = new MongoDbInputMeta();
    meta.setConnectionName("test-connection");
    meta.setCollection(""); // Empty collection

    MongoDbInputData data = new MongoDbInputData();
    MongoDbInput transform = createTransform(meta, data);

    // init() catches exceptions and returns false instead of throwing
    assertFalse(transform.init());
  }

  @Test
  void testDispose() throws HopException, MongoDbException {
    MongoDbInputMeta meta = new MongoDbInputMeta();
    MongoDbInputData data = new MongoDbInputData();
    MongoDbInput transform = createTransform(meta, data);

    // Test dispose with null cursor
    transform.dispose();
    // Should not throw exception

    // Test dispose with mock cursor
    data.cursor = mock(MongoCursorWrapper.class);
    transform.dispose();
    verify(data.cursor).close();
  }

  private MongoDbInput createTransform(MongoDbInputMeta meta, MongoDbInputData data)
      throws HopException, MongoDbException {
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    MongoDbInput transform =
        new MongoDbInput(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
    transform.setMetadataProvider(metadataProvider);
    return transform;
  }
}
