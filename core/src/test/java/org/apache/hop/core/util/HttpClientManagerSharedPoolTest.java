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

package org.apache.hop.core.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.junit.jupiter.api.Test;

/**
 * Guards <a href="https://github.com/apache/hop/issues/8160">HOP-8160</a>: closing a client built
 * by {@link HttpClientManager} must not shut down the process-wide connection pool.
 */
class HttpClientManagerSharedPoolTest {

  @Test
  void closingADefaultClientLeavesTheSharedPoolUsable() throws Exception {
    CloseableHttpClient first = HttpClientManager.getInstance().createDefaultClient();
    first.close();

    CloseableHttpClient second = HttpClientManager.getInstance().createDefaultClient();
    try {
      assertPoolStillUsable(second);
    } finally {
      second.close();
    }
  }

  @Test
  void closingABuilderClientLeavesTheSharedPoolUsable() throws Exception {
    CloseableHttpClient first = HttpClientManager.getInstance().createBuilder().build();
    first.close();

    CloseableHttpClient second = HttpClientManager.getInstance().createBuilder().build();
    try {
      assertPoolStillUsable(second);
    } finally {
      second.close();
    }
  }

  private static void assertPoolStillUsable(CloseableHttpClient client) throws Exception {
    try {
      client.execute(new HttpGet("http://127.0.0.1:1/"));
    } catch (IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("Connection pool shut down")) {
        fail("Shared connection pool was shut down when a client was closed (HOP-8160)");
      }
      throw e;
    } catch (IOException ignored) {
      // Port 1 is not a real endpoint. Reaching here means the pool still leased a connection.
    }
  }
}
