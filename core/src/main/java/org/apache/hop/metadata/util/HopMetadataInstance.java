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
 *
 */

package org.apache.hop.metadata.util;

import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

public class HopMetadataInstance {
  private static HopMetadataInstance instance;

  private MultiMetadataProvider metadataProvider;

  private HopMetadataInstance() {
    // Nothing to do here.
  }

  public static HopMetadataInstance getInstance() {
    if (instance == null) {
      instance = new HopMetadataInstance();
    }
    return instance;
  }

  public static void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    getInstance().metadataProvider = metadataProvider;
  }

  public static MultiMetadataProvider getMetadataProvider() {
    return getInstance().metadataProvider;
  }
}
