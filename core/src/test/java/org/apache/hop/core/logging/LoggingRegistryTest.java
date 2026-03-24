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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link LoggingRegistry} */
class LoggingRegistryTest {
  public static final String LOG_CHANEL_ID_PARENT = "parent-chanel-id";
  public static final String LOG_CHANEL_ID_CHILD = "child-chanel-id";
  public static final String STRING_DEFAULT = "<def>";

  @BeforeEach
  void resetRegistry() {
    LoggingRegistry.getInstance().reset();
  }

  @Test
  void correctLogIdReturned_WhenLogObjectRegisteredAlready() {
    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LoggingObject parent =
        new LoggingObject(new SimpleLoggingObject("parent", LoggingObjectType.PIPELINE, null));
    parent.setLogChannelId(LOG_CHANEL_ID_PARENT);

    LoggingObject child =
        new LoggingObject(new SimpleLoggingObject("child", LoggingObjectType.TRANSFORM, parent));
    child.setLogChannelId(LOG_CHANEL_ID_CHILD);

    loggingRegistry.getMap().put(STRING_DEFAULT, child);

    String logChanelId = loggingRegistry.registerLoggingSource(child);

    assertEquals(LOG_CHANEL_ID_CHILD, logChanelId);
  }

  @Test
  void testRegisterFileWriter() {
    String id = "1";

    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer buffer = new LogChannelFileWriterBuffer(id);
    loggingRegistry.registerLogChannelFileWriterBuffer(buffer);

    assertNotNull(loggingRegistry.getLogChannelFileWriterBuffer(id));
  }

  @Test
  void testFileWritersIds() {
    String id = "1";

    LoggingRegistry loggingRegistry = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer buffer = new LogChannelFileWriterBuffer(id);
    loggingRegistry.registerLogChannelFileWriterBuffer(buffer);

    assertNotNull(loggingRegistry.getLogChannelFileWriterBufferIds());
  }

  @Test
  void testRemoveFileWriter() {
    String id = "1";

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

    LoggingObject outer =
        new LoggingObject(new SimpleLoggingObject("outer", LoggingObjectType.ACTION, null));
    outer.setLogChannelId("outer-id");

    LoggingObject inner =
        new LoggingObject(new SimpleLoggingObject("inner", LoggingObjectType.ACTION, outer));
    inner.setLogChannelId("inner-id");

    LoggingObject pipeline =
        new LoggingObject(new SimpleLoggingObject("pipe", LoggingObjectType.PIPELINE, inner));
    pipeline.setLogChannelId("pipe-id");

    reg.getMap().put("outer-id", outer);
    reg.getMap().put("inner-id", inner);
    reg.getMap().put("pipe-id", pipeline);

    LogChannelFileWriterBuffer outerBuf = new LogChannelFileWriterBuffer("outer-id");
    LogChannelFileWriterBuffer innerBuf = new LogChannelFileWriterBuffer("inner-id");
    reg.registerLogChannelFileWriterBuffer(outerBuf);
    reg.registerLogChannelFileWriterBuffer(innerBuf);

    assertSame(innerBuf, reg.getLogChannelFileWriterBuffer("pipe-id"));
  }

  @Test
  void removeLogChannelFileWriterBuffer_doesNotRemoveDescendantBuffers() {
    LoggingRegistry reg = LoggingRegistry.getInstance();

    LogChannelFileWriterBuffer outerBuf = new LogChannelFileWriterBuffer("outer-id");
    LogChannelFileWriterBuffer innerBuf = new LogChannelFileWriterBuffer("inner-id");
    reg.registerLogChannelFileWriterBuffer(outerBuf);
    reg.registerLogChannelFileWriterBuffer(innerBuf);

    reg.removeLogChannelFileWriterBuffer("outer-id");

    assertNull(reg.getLogChannelFileWriterBuffer("outer-id"));
    assertSame(innerBuf, reg.getLogChannelFileWriterBuffer("inner-id"));
  }
}
