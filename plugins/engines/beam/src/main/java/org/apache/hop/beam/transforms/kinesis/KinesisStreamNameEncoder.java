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

package org.apache.hop.beam.transforms.kinesis;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.metadata.api.IStringEncoder;

/**
 * Stores a Kinesis stream name the way Hop has always stored it: encrypted, as if it were a
 * password.
 *
 * <p>A stream name is not a secret, and marking it {@code password = true} made the linter report
 * every Kinesis transform as holding a hardcoded secret. Dropping the flag outright would change
 * the file format: pipelines saved so far hold the name encrypted, and would read it back as {@code
 * Encrypted 2be98...}. This encoder keeps the stored value, and what older Hop versions read,
 * exactly as before.
 */
public class KinesisStreamNameEncoder implements IStringEncoder {

  @Override
  public String encode(String string) {
    return Encr.encryptPasswordIfNotUsingVariables(string);
  }

  @Override
  public String decode(String string) {
    return Encr.decryptPasswordOptionallyEncrypted(string);
  }
}
