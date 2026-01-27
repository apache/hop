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

package org.apache.hop.pipeline.transforms.rest.common;

import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.hop.pipeline.transforms.rest.RestMeta;

/** rest const */
@UtilityClass
public class RestConst {
  public static final int DEFAULT_RETRY_TIMES = 0;
  public static final long DEFAULT_RETRY_DELAY_MS = 200L;

  /**
   * By default, the HTTP status codes that should be retried include 429, 500, 502, 503, and 504.
   */
  public static Set<String> retryStatusCodes() {
    return Set.of("429", "500", "502", "503", "504");
  }

  /**
   * By default, the HTTP methods that should be retried include GET, PUT, and DELETE, as these
   * methods are considered idempotent and safe to retry.
   */
  public static Set<String> retryMethods() {
    return Set.of(RestMeta.HTTP_METHOD_GET, RestMeta.HTTP_METHOD_DELETE, RestMeta.HTTP_METHOD_PUT);
  }
}
