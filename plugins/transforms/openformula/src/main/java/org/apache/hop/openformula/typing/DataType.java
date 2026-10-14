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

package org.apache.hop.openformula.typing;

import java.io.Serializable;

/** Fundamental data types in the formula system. */
public enum DataType implements Serializable {
  ANY(0),
  TEXT(1),
  NUMBER(2),
  DATETIME(4),
  LOGICAL(8),
  ARRAY(16),
  ERROR(32);

  public static final int ARRAY_TYPE = 16;

  private final int flag;

  DataType(int flag) {
    this.flag = flag;
  }

  public int getFlag() {
    return flag;
  }

  public boolean isFlagSet(int flag) {
    return (this.flag & flag) != 0;
  }
}
