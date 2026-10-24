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

package org.apache.hop.metadata.serializer.unknownproperty;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.OnUnknownDeserializationProperty;
import org.apache.hop.metadata.api.UnknownProperty;

/** Test metadata type used to verify the {@link OnUnknownDeserializationProperty} callback. */
@HopMetadata(name = "PropertyWithLegacy", key = "property-with-legacy")
public class PropertyWithLegacy extends HopMetadataBase implements IHopMetadata {

  /** The 'new' field, filled from annotated data. */
  @HopMetadataProperty private String value;

  /** Captures every unknown property seen by the callback, in insertion order. */
  private final Map<String, String> unknownProperties = new LinkedHashMap<>();

  @OnUnknownDeserializationProperty
  public void onUnknown(UnknownProperty property) {
    unknownProperties.put(property.getName(), property.getValue());
  }

  /**
   * @return value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue(String value) {
    this.value = value;
  }

  public String getUnknown(String name) {
    return unknownProperties.get(name);
  }

  public Map<String, String> getUnknownProperties() {
    return unknownProperties;
  }
}
