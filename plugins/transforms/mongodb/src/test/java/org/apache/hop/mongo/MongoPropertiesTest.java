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

package org.apache.hop.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MongoPropertiesTest {
  @Test
  void testBuildsMongoClientSettings() throws Exception {
    MongoProperties props =
        new MongoProperties.Builder()
            .set(MongoProp.connectionsPerHost, "127")
            .set(MongoProp.connectTimeout, "333")
            .set(MongoProp.maxWaitTime, "12345")
            .set(MongoProp.cursorFinalizerEnabled, "false")
            .set(MongoProp.socketKeepAlive, "true")
            .set(MongoProp.socketTimeout, "4")
            .set(MongoProp.useSSL, "true")
            .set(MongoProp.readPreference, "primary")
            .set(MongoProp.USE_ALL_REPLICA_SET_MEMBERS, "false")
            .build();
    MongoUtilLogger log = Mockito.mock(MongoUtilLogger.class);
    MongoClientSettings.Builder settingsBuilder = props.buildMongoClientSettings(log);
    assertNotNull(settingsBuilder);
    MongoClientSettings settings = settingsBuilder.build();
    // Verify settings were built - exact assertions depend on what's accessible
    assertNotNull(settings.getReadPreference());
    assertEquals(props.getReadPreference(), ReadPreference.primary());
    assertFalse(props.useAllReplicaSetMembers());
    assertEquals(
        "MongoProperties:\n"
            + "connectionsPerHost=127\n"
            + "connectTimeout=333\n"
            + "cursorFinalizerEnabled=false\n"
            + "HOST=localhost\n"
            + "maxWaitTime=12345\n"
            + "PASSWORD=\n"
            + "readPreference=primary\n"
            + "socketKeepAlive=true\n"
            + "socketTimeout=4\n"
            + "USE_ALL_REPLICA_SET_MEMBERS=false\n"
            + "useSSL=true\n",
        props.toString());
  }

  @Test
  void testBuildsMongoClientSettingsDefaults() throws Exception {
    MongoProperties props = new MongoProperties.Builder().build();
    MongoUtilLogger log = Mockito.mock(MongoUtilLogger.class);
    MongoClientSettings.Builder settingsBuilder = props.buildMongoClientSettings(log);
    assertNotNull(settingsBuilder);
    MongoClientSettings settings = settingsBuilder.build();
    // Basic assertion that settings were created
    assertNotNull(settings);
  }
}
