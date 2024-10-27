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

package org.apache.hop.neo4j.transforms.output.fields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class PropertyField {

  @HopMetadataProperty(key = "name", injectionKey = "NODE_PROPERTY_NAME")
  private String propertyName;

  @HopMetadataProperty(key = "value", injectionKey = "NODE_PROPERTY_VALUE")
  private String propertyValue;

  @HopMetadataProperty(key = "type", injectionKey = "NODE_PROPERTY_TYPE")
  private String propertyType;

  @HopMetadataProperty(key = "primary", injectionKey = "NODE_PROPERTY_PRIMARY")
  private boolean propertyPrimary;
}
