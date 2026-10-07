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
package org.apache.hop.vfs.smb;

import com.hierynomus.mssmb2.SMB2Dialect;
import java.util.List;

/**
 * Lowest SMB dialect this connection will offer. The ceiling is always SMB 3.1.1. SMB1 is never in
 * the list: smbj does not speak it.
 */
public enum SmbDialect {
  SMB_2_0_2,
  SMB_3_0,
  SMB_3_1_1;

  /** Dialects from this floor through SMB 3.1.1, lowest first. */
  public List<SMB2Dialect> negotiated() {
    return switch (this) {
      case SMB_3_1_1 -> List.of(SMB2Dialect.SMB_3_1_1);
      case SMB_3_0 -> List.of(SMB2Dialect.SMB_3_0, SMB2Dialect.SMB_3_0_2, SMB2Dialect.SMB_3_1_1);
      case SMB_2_0_2 ->
          List.of(
              SMB2Dialect.SMB_2_0_2,
              SMB2Dialect.SMB_2_1,
              SMB2Dialect.SMB_3_0,
              SMB2Dialect.SMB_3_0_2,
              SMB2Dialect.SMB_3_1_1);
    };
  }
}
