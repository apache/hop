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
 *
 */

package org.apache.hop.vfs.minio.config;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MinioConfig {

  public static final String HOP_CONFIG_MINIO_CONFIG_KEY = "minio";

  private String endPointHostname;
  private String endPointPort;
  private boolean endPointSecure;
  private String accessKey;
  private String secretKey;
  private String region;
  private String partSize;

  public MinioConfig() {}

  public MinioConfig(MinioConfig config) {
    this();
    this.endPointHostname = config.endPointHostname;
    this.endPointPort = config.endPointPort;
    this.endPointSecure = config.endPointSecure;
    this.accessKey = config.accessKey;
    this.secretKey = config.secretKey;
    this.region = config.region;
    this.partSize = config.partSize;
  }
}
