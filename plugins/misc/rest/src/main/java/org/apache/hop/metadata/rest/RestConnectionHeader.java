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

package org.apache.hop.metadata.rest;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * A header a REST connection sends on every request (issue #9551), for example the {@code
 * x-as-user-email} an API wants next to its OAuth 2 token. Name and value can use variables.
 */
@Getter
@Setter
@NoArgsConstructor
public class RestConnectionHeader {

  @HopMetadataProperty(key = "name")
  private String name;

  /**
   * The value. Static headers often carry something credential-like (an {@code x-api-key}, say), so
   * the value is stored encrypted, like the connection's other secrets. A value that was saved in
   * plain text still loads.
   */
  @HopMetadataProperty(key = "value", password = true)
  private String value;

  public RestConnectionHeader(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public RestConnectionHeader(RestConnectionHeader header) {
    this(header.name, header.value);
  }
}
