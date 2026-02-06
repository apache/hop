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

package org.apache.hop.pipeline.transforms.httppost;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class HttpPostArgumentField {

  @HopMetadataProperty(injectionKeyDescription = "HTTPPOST.Injection.ArgumentFieldName")
  private String name;

  @HopMetadataProperty(injectionKeyDescription = "HTTPPOST.Injection.ArgumentFieldParameter")
  private String parameter;

  @HopMetadataProperty(injectionKeyDescription = "HTTPPOST.Injection.ArgumentFieldHeader")
  private boolean header;

  public HttpPostArgumentField(String name, String parameter, boolean header) {
    this.name = name;
    this.parameter = parameter;
    this.header = header;
  }

  public HttpPostArgumentField(HttpPostArgumentField httpPostArgumentField) {
    this.name = httpPostArgumentField.name;
    this.parameter = httpPostArgumentField.parameter;
    this.header = httpPostArgumentField.header;
  }

  public HttpPostArgumentField() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpPostArgumentField that = (HttpPostArgumentField) o;
    return header == that.header
        && Objects.equals(name, that.name)
        && Objects.equals(parameter, that.parameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parameter, header);
  }
}
