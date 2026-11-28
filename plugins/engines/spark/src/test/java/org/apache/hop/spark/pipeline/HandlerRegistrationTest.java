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

package org.apache.hop.spark.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The engine runs without the plugin of a transform that a pipeline doesn't use. */
class HandlerRegistrationTest {

  /**
   * A constructor reference is linked where it is written, which loads the handler's class: with
   * its transform plugin missing, the converter would fail before register() can skip the handler.
   */
  @Test
  void handlersAreMadeInsideRegister() throws Exception {
    String source =
        Files.readString(
            Path.of(
                "src/main/java/org/apache/hop/spark/pipeline/HopPipelineMetaToSparkConverter.java"));
    int start = source.indexOf("protected void addDefaultTransformHandlers()");
    String body = source.substring(start, source.indexOf("\n  }\n", start));
    assertFalse(body.contains("::new"), "a constructor reference in addDefaultTransformHandlers");
  }
}
