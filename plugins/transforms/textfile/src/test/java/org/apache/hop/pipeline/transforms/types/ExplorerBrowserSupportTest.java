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

package org.apache.hop.pipeline.transforms.types;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ExplorerBrowserSupportTest {

  @Test
  void httpUrls() {
    assertTrue(ExplorerBrowserSupport.isHttpUrl("https://hop.apache.org/manual"));
    assertTrue(ExplorerBrowserSupport.isHttpUrl("HTTP://example.com/a.html"));
    assertFalse(ExplorerBrowserSupport.isHttpUrl("/project/docs/index.html"));
    assertFalse(ExplorerBrowserSupport.isHttpUrl("file:///tmp/index.html"));
    assertFalse(ExplorerBrowserSupport.isHttpUrl(null));
  }

  @Test
  void browserFetchableSchemes() {
    assertTrue(ExplorerBrowserSupport.isBrowserFetchable("file:///tmp/index.html"));
    assertTrue(ExplorerBrowserSupport.isBrowserFetchable("https://example.com/a.html"));
    assertTrue(ExplorerBrowserSupport.isBrowserFetchable("http://localhost/a.html"));
    assertFalse(ExplorerBrowserSupport.isBrowserFetchable("s3://bucket/docs/index.html"));
    assertFalse(ExplorerBrowserSupport.isBrowserFetchable(null));
  }
}
