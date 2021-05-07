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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class BlockingTransform {

  @HopMetadataProperty private String name;

  @HopMetadataProperty(key = "CopyNr")
  private String copyNr;

  public BlockingTransform() {}

  public BlockingTransform(BlockingTransform t) {
    this.name = t.name;
    this.copyNr = t.copyNr;
  }

  public BlockingTransform(String name, String copyNr) {
    this.name = name;
    this.copyNr = copyNr;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  public String getCopyNr() {
    return copyNr;
  }

  /** @param copyNr The copyNr to set */
  public void setCopyNr(String copyNr) {
    this.copyNr = copyNr;
  }
}
