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

package org.apache.hop.pipeline.transforms.cratedbbulkloader.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpResponse;
import org.apache.hop.core.exception.HopException;

public class HttpBulkImportResponse {

  private final long outputRows;

  private final int statusCode;

  private final long rejectedRows;

  private HttpBulkImportResponse(int statusCode, Long outputRows, Long rejectedRows) {
    this.statusCode = statusCode;
    this.outputRows = outputRows;
    this.rejectedRows = rejectedRows;
  }

  public static HttpBulkImportResponse fromHttpResponse(HttpResponse<String> response)
      throws HopException, IOException {
    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      CrateDBHttpResponse crateDBHttpResponse =
          mapper.readValue(response.body(), CrateDBHttpResponse.class);
      long outputRows =
          crateDBHttpResponse.results.parallelStream()
              .filter(r -> r.getRowCount() > 0)
              .mapToInt(RowMetrics::getRowCount)
              .sum();
      long rejectedRows =
          crateDBHttpResponse.results.parallelStream()
              .filter(r -> r.getRowCount() < 0)
              .mapToInt(RowMetrics::getRowCount)
              .count();
      return new HttpBulkImportResponse(response.statusCode(), outputRows, rejectedRows);
    } catch (JsonProcessingException e) {
      throw new HopException("Unable to parse body of response", e);
    }
  }

  public long outputRows() {
    return outputRows;
  }

  public int statusCode() {
    return statusCode;
  }

  public long rejectedRows() {
    return rejectedRows;
  }
}
