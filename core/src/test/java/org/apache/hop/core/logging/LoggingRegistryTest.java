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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Unit test for {@link LoggingRegistry} */
class LoggingRegistryTest {

  @Test
  void correctLogIdReturned_WhenLogObjectRegisteredAlready() {
    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    String parentChannelId = UUID.randomUUID().toString();
    String childChannelId = UUID.randomUUID().toString();
    String mapKey = UUID.randomUUID().toString();

    LoggingObject parent =
        new LoggingObject(new SimpleLoggingObject("parent", LoggingObjectType.PIPELINE, null));
    parent.setLogChannelId(parentChannelId);

    LoggingObject child =
        new LoggingObject(new SimpleLoggingObject("child", LoggingObjectType.TRANSFORM, parent));
    child.setLogChannelId(childChannelId);

    loggingRegistry.getMap().put(mapKey, child);

    String logChanelId = loggingRegistry.registerLoggingSource(child);

    assertEquals(childChannelId, logChanelId);
  }
}
