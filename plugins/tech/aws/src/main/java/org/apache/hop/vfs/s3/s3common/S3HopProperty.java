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

package org.apache.hop.vfs.s3.s3common;

import org.apache.hop.core.config.HopConfig;

/** Class that handles operations dealing with hop property file. */
public class S3HopProperty {
  // TODO: figure out a way to allow this plugin to add default values to the global list of
  // variables
  //
  public static final String S3VFS_PART_SIZE = "HOP_S3_VFS_PART_SIZE";

  public String getPartSize() {
    return HopConfig.getInstance().findDescribedVariableValue(S3VFS_PART_SIZE);
  }
}
