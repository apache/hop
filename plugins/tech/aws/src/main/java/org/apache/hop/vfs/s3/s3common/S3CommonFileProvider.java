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

package org.apache.hop.vfs.s3.s3common;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public abstract class S3CommonFileProvider extends AbstractOriginatingFileProvider {

  /** Authentication types. */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
      new UserAuthenticationData.Type[] {
        UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
      };

  /** The provider's capabilities. */
  protected static final Collection<Capability> capabilities =
      Collections.unmodifiableCollection(
          Arrays.asList(
              Capability.CREATE,
              Capability.DELETE,
              Capability.RENAME,
              Capability.GET_TYPE,
              Capability.LIST_CHILDREN,
              Capability.READ_CONTENT,
              Capability.URI,
              Capability.WRITE_CONTENT,
              Capability.GET_LAST_MODIFIED,
              Capability.RANDOM_ACCESS_READ));

  public S3CommonFileProvider() {
    super();
  }

  @Override
  protected abstract FileSystem doCreateFileSystem(
      final FileName name, final FileSystemOptions fileSystemOptions) throws FileSystemException;

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
