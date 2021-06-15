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

package org.apache.hop.core.injection.metadata;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class PropBeanListChild {

  @HopMetadataProperty private String f1;

  @HopMetadataProperty private String f2;

  public PropBeanListChild() {}

  public PropBeanListChild(String f1, String f2) {
    this.f1 = f1;
    this.f2 = f2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PropBeanListChild that = (PropBeanListChild) o;
    return Objects.equals(f1, that.f1) && Objects.equals(f2, that.f2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(f1, f2);
  }

  /**
   * Gets f1
   *
   * @return value of f1
   */
  public String getF1() {
    return f1;
  }

  /** @param f1 The f1 to set */
  public void setF1(String f1) {
    this.f1 = f1;
  }

  /**
   * Gets f2
   *
   * @return value of f2
   */
  public String getF2() {
    return f2;
  }

  /** @param f2 The f2 to set */
  public void setF2(String f2) {
    this.f2 = f2;
  }
}
