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

package org.apache.hop.workflow.actions.addresultfilenames;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class Argument {

  @HopMetadataProperty(key = "name")
  private String argument;

  @HopMetadataProperty(key = "filemask")
  private String mask;

  public Argument() {}

  public Argument(String argument, String mask) {
    this.argument = argument;
    this.mask = mask;
  }

  /**
   * Gets argument
   *
   * @return value of argument
   */
  public String getArgument() {
    return argument;
  }

  /** @param argument The argument to set */
  public void setArgument(String argument) {
    this.argument = argument;
  }

  /**
   * Gets mask
   *
   * @return value of mask
   */
  public String getMask() {
    return mask;
  }

  /** @param mask The mask to set */
  public void setMask(String mask) {
    this.mask = mask;
  }
}
