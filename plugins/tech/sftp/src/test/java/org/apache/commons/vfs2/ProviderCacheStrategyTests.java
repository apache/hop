/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.VirtualFileSystem;
import org.apache.commons.vfs2.provider.ram.RamFileObject;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.junit.jupiter.api.Test;

/** Test the cache strategy. */
public class ProviderCacheStrategyTests extends AbstractProviderTestCase {

  public void assertContains(final FileObject[] fos, final String string) {
    for (final FileObject fo : fos) {
      if (string.equals(fo.getName().getBaseName())) {
        return;
      }
    }

    fail(string + " should be seen");
  }

  public void assertContainsNot(final FileObject[] fos, final String string) {
    for (final FileObject fo : fos) {
      if (string.equals(fo.getName().getBaseName())) {
        fail(string + " should not be seen");
      }
    }
  }

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {
      Capability.CREATE, Capability.GET_TYPE, Capability.LIST_CHILDREN,
    };
  }

  /** Test the manual cache strategy. */
  @Test
  public void testManualCache() throws Exception {
    final FileObject scratchFolder = getWriteFolder();
    if (FileObjectUtils.isInstanceOf(getBaseFolder(), RamFileObject.class)
        || scratchFolder.getFileSystem() instanceof VirtualFileSystem) {
      // can't check ram filesystem as every manager holds its own ram filesystem data
      return;
    }

    scratchFolder.delete(Selectors.EXCLUDE_SELF);

    final DefaultFileSystemManager fs = createManager();
    fs.setCacheStrategy(CacheStrategy.MANUAL);
    fs.init();
    final FileObject foBase2 = getBaseTestFolder(fs);

    final FileObject cachedFolder = foBase2.resolveFile(scratchFolder.getName().getPath());

    FileObject[] fos = cachedFolder.getChildren();
    assertContainsNot(fos, "file1.txt");

    scratchFolder.resolveFile("file1.txt").createFile();

    fos = cachedFolder.getChildren();
    assertContainsNot(fos, "file1.txt");

    cachedFolder.refresh();
    fos = cachedFolder.getChildren();
    assertContains(fos, "file1.txt");
  }

  /** Test the on_call strategy. */
  @Test
  public void testOnCallCache() throws Exception {
    final FileObject scratchFolder = getWriteFolder();
    if (FileObjectUtils.isInstanceOf(getBaseFolder(), RamFileObject.class)
        || scratchFolder.getFileSystem() instanceof VirtualFileSystem) {
      // can't check ram filesystem as every manager holds its own ram filesystem data
      return;
    }

    scratchFolder.delete(Selectors.EXCLUDE_SELF);

    final DefaultFileSystemManager fs = createManager();
    fs.setCacheStrategy(CacheStrategy.ON_CALL);
    fs.init();
    final FileObject foBase2 = getBaseTestFolder(fs);

    final FileObject cachedFolder = foBase2.resolveFile(scratchFolder.getName().getPath());

    FileObject[] fos = cachedFolder.getChildren();
    assertContainsNot(fos, "file1.txt");

    scratchFolder.resolveFile("file1.txt").createFile();

    fos = cachedFolder.getChildren();
    assertContains(fos, "file1.txt");
  }

  /** Test the on_resolve strategy. */
  @Test
  public void testOnResolveCache() throws Exception {
    final FileObject scratchFolder = getWriteFolder();
    if (FileObjectUtils.isInstanceOf(getBaseFolder(), RamFileObject.class)
        || scratchFolder.getFileSystem() instanceof VirtualFileSystem) {
      // can't check ram filesystem as every manager holds its own ram filesystem data
      return;
    }

    scratchFolder.delete(Selectors.EXCLUDE_SELF);

    final DefaultFileSystemManager fs = createManager();
    fs.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    fs.init();
    final FileObject foBase2 = getBaseTestFolder(fs);

    FileObject cachedFolder = foBase2.resolveFile(scratchFolder.getName().getPath());

    FileObject[] fos = cachedFolder.getChildren();
    assertContainsNot(fos, "file1.txt");

    scratchFolder.resolveFile("file1.txt").createFile();

    fos = cachedFolder.getChildren();
    assertContainsNot(fos, "file1.txt");

    cachedFolder = foBase2.resolveFile(scratchFolder.getName().getPath());
    fos = cachedFolder.getChildren();
    assertContains(fos, "file1.txt");
  }
}
