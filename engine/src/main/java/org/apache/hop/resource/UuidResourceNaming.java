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

package org.apache.hop.resource;

import java.util.UUID;

public class UuidResourceNaming extends SimpleResourceNaming {

  //
  // End result could look like any of the following:
  //
  // Inputs:
  // Prefix : Marc Sample Pipeline
  // Original Path: D:\japps\hop\samples
  // Extension : .hpl
  //
  // Output Example 1 (no file system prefix, no path used)
  // Marc_Sample_Pipeline_03a32f25-1538-11dc-ae07-5dbf1395f3fd.hpl
  // Output Example 2 (file system prefix: ${HOP_FILE_BASE}!, no path used)
  // ${HOP_FILE_BASE}!Marc_Sample_Pipeline_03a32f25-1538-11dc-ae07-5dbf1395f3fd.hpl
  // Output Example 3 (file system prefix: ${HOP_FILE_BASE}!, path is used)
  // ${HOP_FILE_BASE}!japps/hop/samples/
  //   Marc_Sample_Pipeline_03a32f25-1538-11dc-ae07-5dbf1395f3fd.hpl

  protected String getFileNameUniqueIdentifier() {
    // This implementation assumes that the name alone
    // will be insufficient to uniquely identify the
    // file. So, return a UUID which will be used
    // to guarentee uniqueness.
    //
    // The UUID will look something like this:
    // 03a32f25-1538-11dc-ae07-5dbf1395f3fd
    //
    return UUID.randomUUID().toString();
  }

}
