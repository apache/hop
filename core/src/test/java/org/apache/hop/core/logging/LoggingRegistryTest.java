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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

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

  @Test
  void testRegisterFileWriter() {
    String id = UUID.randomUUID().toString();

    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer buffer = new LogChannelFileWriterBuffer(id);
    loggingRegistry.registerLogChannelFileWriterBuffer(buffer);

    assertNotNull(loggingRegistry.getLogChannelFileWriterBuffer(id));
  }

  @Test
  void testFileWritersIds() {
    String id = UUID.randomUUID().toString();

    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer buffer = new LogChannelFileWriterBuffer(id);
    loggingRegistry.registerLogChannelFileWriterBuffer(buffer);

    assertNotNull(loggingRegistry.getLogChannelFileWriterBufferIds());
  }

  @Test
  void testRemoveFileWriter() {
    String id = UUID.randomUUID().toString();

    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer buffer = new LogChannelFileWriterBuffer(id);
    loggingRegistry.registerLogChannelFileWriterBuffer(buffer);

    loggingRegistry.removeLogChannelFileWriterBuffer(id);

    assertNull(loggingRegistry.getLogChannelFileWriterBuffer(id));
  }

  /**
   * Nested workflow/pipeline actions can each specify a log file; the pipeline must use the
   * innermost action's buffer, not an arbitrary ancestor match.
   */
  @Test
  void nestedFileWriterBuffers_resolveInnermostAncestor() {
    LoggingRegistry reg = LoggingRegistry.getInstance();

    String outerId = UUID.randomUUID().toString();
    String innerId = UUID.randomUUID().toString();
    String pipeId = UUID.randomUUID().toString();

    LoggingObject outer =
        new LoggingObject(new SimpleLoggingObject("outer", LoggingObjectType.ACTION, null));
    outer.setLogChannelId(outerId);

    LoggingObject inner =
        new LoggingObject(new SimpleLoggingObject("inner", LoggingObjectType.ACTION, outer));
    inner.setLogChannelId(innerId);

    LoggingObject pipeline =
        new LoggingObject(new SimpleLoggingObject("pipe", LoggingObjectType.PIPELINE, inner));
    pipeline.setLogChannelId(pipeId);

    reg.getMap().put(outerId, outer);
    reg.getMap().put(innerId, inner);
    reg.getMap().put(pipeId, pipeline);

    LogChannelFileWriterBuffer outerBuf = new LogChannelFileWriterBuffer(outerId);
    LogChannelFileWriterBuffer innerBuf = new LogChannelFileWriterBuffer(innerId);
    reg.registerLogChannelFileWriterBuffer(outerBuf);
    reg.registerLogChannelFileWriterBuffer(innerBuf);

    assertSame(innerBuf, reg.getLogChannelFileWriterBuffer(pipeId));
  }

  @Test
  void removeLogChannelFileWriterBuffer_doesNotRemoveDescendantBuffers() {
    LoggingRegistry reg = LoggingRegistry.getInstance();

    String outerId = UUID.randomUUID().toString();
    String innerId = UUID.randomUUID().toString();

    LogChannelFileWriterBuffer outerBuf = new LogChannelFileWriterBuffer(outerId);
    LogChannelFileWriterBuffer innerBuf = new LogChannelFileWriterBuffer(innerId);
    reg.registerLogChannelFileWriterBuffer(outerBuf);
    reg.registerLogChannelFileWriterBuffer(innerBuf);

    reg.removeLogChannelFileWriterBuffer(outerId);

    assertNull(reg.getLogChannelFileWriterBuffer(outerId));
    assertSame(innerBuf, reg.getLogChannelFileWriterBuffer(innerId));
  }
}
