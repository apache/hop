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

package org.apache.hop.ui.core.bus;

import java.util.Objects;

public class HopGuiEvent<T> {

  private String id;
  private T subject;

  public HopGuiEvent(String id, T subject) {
    this.id = id;
    this.subject = subject;
  }

  public HopGuiEvent(String id) {
    this(id, null);
  }

  @Override
  public String toString() {
    return "HopGuiEvent{" + "id='" + id + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HopGuiEvent<?> that = (HopGuiEvent<?>) o;
    return Objects.equals(id, that.id) && Objects.equals(subject, that.subject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, subject);
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public T getSubject() {
    return subject;
  }

  /** @param subject The subject to set */
  public void setSubject(T subject) {
    this.subject = subject;
  }
}
