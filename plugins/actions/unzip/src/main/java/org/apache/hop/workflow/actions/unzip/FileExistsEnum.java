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

package org.apache.hop.workflow.actions.unzip;

import org.apache.hop.metadata.api.IEnumHasCode;

public enum FileExistsEnum implements IEnumHasCode {
  SKIP(0),
  OVERWRITE(1),
  UNIQ(2),
  FAIL(3),
  OVERWRITE_DIFF_SIZE(4),
  OVERWRITE_EQUAL_SIZE(5),
  OVERWRITE_ZIP_BIG(6),
  OVERWRITE_ZIP_BIG_EQUAL(7),
  OVERWRITE_ZIP_BIG_SMALL(8),
  OVERWRITE_ZIP_BIG_SMALL_EQUAL(9);

  private final int code;

  FileExistsEnum(int code) {
    this.code = code;
  }

  public int getOrignalCode() {
    return code;
  }

  public String getCode() {
    return String.valueOf(code);
  }

  public static FileExistsEnum getFileExistsEnum(int code) {
    for (FileExistsEnum f : FileExistsEnum.values()) {
      if (f.code == code) {
        return f;
      }
    }
    return null;
  }
}
