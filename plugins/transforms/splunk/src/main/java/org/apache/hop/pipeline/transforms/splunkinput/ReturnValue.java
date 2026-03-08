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

package org.apache.hop.pipeline.transforms.splunkinput;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class ReturnValue {
  @HopMetadataProperty(
      key = "return_name",
      injectionKey = "return_name",
      injectionKeyDescription = "SplunkInputMeta.Injection.Return.Name")
  private String name;

  @HopMetadataProperty(
      key = "return_splunk_name",
      injectionKey = "return_splunk_name",
      injectionKeyDescription = "SplunkInputMeta.Injection.Return.SplunkName")
  private String splunkName;

  @HopMetadataProperty(
      key = "return_type",
      injectionKey = "return_type",
      injectionKeyDescription = "SplunkInputMeta.Injection.Return.Type")
  private String type;

  @HopMetadataProperty(
      key = "return_length",
      injectionKey = "return_length",
      injectionKeyDescription = "SplunkInputMeta.Injection.Return.Length")
  private int length;

  @HopMetadataProperty(
      key = "return_format",
      injectionKey = "return_format",
      injectionKeyDescription = "SplunkInputMeta.Injection.Return.Format")
  private String format;

  public ReturnValue() {}

  public ReturnValue(String name, String splunkName, String type, int length, String format) {
    this.name = name;
    this.splunkName = splunkName;
    this.type = type;
    this.length = length;
    this.format = format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReturnValue that = (ReturnValue) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "ReturnValue{" + "name='" + name + '\'' + '}';
  }
}
