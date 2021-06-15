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

package org.apache.hop.vfs.azure;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.UriParser;

public class AzureFileName extends AbstractFileName {

  protected AzureFileName(final String scheme, final String path, final FileType type) {
    super(scheme, path, type);
  }

  public String getContainer() {
    StringBuilder bui = new StringBuilder(getPath());
    return UriParser.extractFirstElement(bui);
  }

  public String getPathAfterContainer() {
    StringBuilder bui = new StringBuilder(getPath());
    UriParser.extractFirstElement(bui);
    return bui.toString();
  }

  @Override
  public FileName createName(String absPath, FileType type) {
    return new AzureFileName(getScheme(), absPath, type);
  }

  @Override
  protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
    buffer.append(getScheme());
    buffer.append("://");
  }
}
