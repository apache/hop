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
package org.apache.hop.vfs.hdfs;

/**
 * How this named HDFS connection talks to the cluster. All three use the WebHDFS REST API. None of
 * them load Hadoop client JARs.
 */
public enum HdfsTransport {
  /** Cloudera HttpFS gateway: one HTTP endpoint, data does not redirect to DataNodes. */
  HttpFS(14000, false, true),
  /** Cloudera Knox topology that fronts WebHDFS. */
  Knox(8443, true, true),
  /**
   * NameNode WebHDFS. CREATE/OPEN 307-redirect to DataNodes, so every DataNode HTTP port must be
   * reachable from the client.
   */
  WebHDFS(9870, false, false);

  private final int defaultPort;
  private final boolean defaultHttps;
  private final boolean dataOnCreateRequest;

  HdfsTransport(int defaultPort, boolean defaultHttps, boolean dataOnCreateRequest) {
    this.defaultPort = defaultPort;
    this.defaultHttps = defaultHttps;
    this.dataOnCreateRequest = dataOnCreateRequest;
  }

  public int defaultPort() {
    return defaultPort;
  }

  public boolean defaultHttps() {
    return defaultHttps;
  }

  /**
   * HttpFS and Knox accept the file body on the CREATE request ({@code data=true}). WebHDFS needs a
   * redirect to a DataNode first.
   */
  public boolean dataOnCreateRequest() {
    return dataOnCreateRequest;
  }

  public static HdfsTransport fromCode(String code) {
    if (code == null || code.isBlank()) {
      return HttpFS;
    }
    String trimmed = code.trim();
    for (HdfsTransport value : values()) {
      if (value.name().equalsIgnoreCase(trimmed)) {
        return value;
      }
    }
    return HttpFS;
  }
}
