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
package org.apache.hop.vfs.hdfs;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;

@Getter
@Setter
public class HdfsFileSystem extends AbstractFileSystem {

  public static final List<Capability> CAPABILITIES =
      Arrays.asList(
          Capability.CREATE,
          Capability.DELETE,
          Capability.RENAME,
          Capability.GET_TYPE,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT,
          Capability.GET_LAST_MODIFIED);

  private HdfsWebHdfsClient client;
  private String defaultRoot = "";
  private ExecutorService executor;

  protected HdfsFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  public synchronized ExecutorService getExecutor() {
    if (executor == null) {
      executor =
          Executors.newCachedThreadPool(
              runnable -> {
                Thread thread = new Thread(runnable, "hop-hdfs-io");
                thread.setDaemon(true);
                return thread;
              });
    }
    return executor;
  }

  public String toHdfsPath(FileName name) {
    String path = name.getPath();
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    if (defaultRoot == null || defaultRoot.isBlank() || "/".equals(defaultRoot)) {
      return path;
    }
    String root =
        defaultRoot.endsWith("/")
            ? defaultRoot.substring(0, defaultRoot.length() - 1)
            : defaultRoot;
    if (!root.startsWith("/")) {
      root = "/" + root;
    }
    if ("/".equals(path)) {
      return root;
    }
    return root + path;
  }

  @Override
  protected void addCapabilities(Collection<Capability> caps) {
    caps.addAll(CAPABILITIES);
  }

  @Override
  protected FileObject createFile(AbstractFileName name) {
    return new HdfsFileObject(name, this);
  }

  @Override
  protected void doCloseCommunicationLink() {
    if (client != null) {
      client.close();
      client = null;
    }
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }
}
