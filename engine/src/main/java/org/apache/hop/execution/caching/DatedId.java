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

package org.apache.hop.execution.caching;

import java.util.Date;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatedId {
  private String id;
  private Date date;

  public DatedId() {
    // Nothing to set
  }

  public DatedId(String id, Date date) {
    this.id = id;
    this.date = date;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DatedId)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    return id.equals(((DatedId) obj).id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
