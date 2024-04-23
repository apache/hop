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
 *
 */

package org.apache.hop.execution.sampler;

import org.apache.commons.lang.StringUtils;

/** Extra metadata about the data sampler being used */
public class ExecutionDataSamplerMeta {
  private String transformName;
  private String copyNr;
  private String logChannelId;
  private boolean firstTransform;
  private boolean lastTransform;

  public ExecutionDataSamplerMeta() {}

  public ExecutionDataSamplerMeta(ExecutionDataSamplerMeta meta) {
    this.transformName = meta.transformName;
    this.copyNr = meta.copyNr;
    this.logChannelId = meta.logChannelId;
    this.firstTransform = meta.firstTransform;
    this.lastTransform = meta.lastTransform;
  }

  public ExecutionDataSamplerMeta(
      String transformName,
      String copyNr,
      String logChannelId,
      boolean firstTransform,
      boolean lastTransform) {
    this.transformName = transformName;
    this.copyNr = copyNr;
    this.logChannelId = logChannelId;
    this.firstTransform = firstTransform;
    this.lastTransform = lastTransform;
  }

  @Override
  public String toString() {
    if (StringUtils.isEmpty(copyNr)) {
      return transformName;
    } else {
      return transformName + "." + copyNr;
    }
  }

  /**
   * Gets transformName
   *
   * @return value of transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * Sets transformName
   *
   * @param transformName value of transformName
   */
  public void setTransformName(String transformName) {
    this.transformName = transformName;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  public String getCopyNr() {
    return copyNr;
  }

  /**
   * Sets copyNr
   *
   * @param copyNr value of copyNr
   */
  public void setCopyNr(String copyNr) {
    this.copyNr = copyNr;
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * Sets logChannelId
   *
   * @param logChannelId value of logChannelId
   */
  public void setLogChannelId(String logChannelId) {
    this.logChannelId = logChannelId;
  }

  /**
   * Gets firstTransform
   *
   * @return value of firstTransform
   */
  public boolean isFirstTransform() {
    return firstTransform;
  }

  /**
   * Sets firstTransform
   *
   * @param firstTransform value of firstTransform
   */
  public void setFirstTransform(boolean firstTransform) {
    this.firstTransform = firstTransform;
  }

  /**
   * Gets lastTransform
   *
   * @return value of lastTransform
   */
  public boolean isLastTransform() {
    return lastTransform;
  }

  /**
   * Sets lastTransform
   *
   * @param lastTransform value of lastTransform
   */
  public void setLastTransform(boolean lastTransform) {
    this.lastTransform = lastTransform;
  }
}
