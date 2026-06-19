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

package org.apache.hop.core.parameters;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Target class for the parameter keys. */
@Getter
@Setter
public class NamedParameter {
  /** key of this parameter */
  @HopMetadataProperty(key = "name")
  protected String key;

  /** Description of the parameter */
  @HopMetadataProperty(key = "description")
  protected String description;

  /** Default value for this parameter */
  @HopMetadataProperty(key = "default_value")
  protected String defaultValue;

  /** Actual value of the parameter. */
  protected String value;

  public NamedParameter() {
    // Nothing specific to initialize.
  }
}
