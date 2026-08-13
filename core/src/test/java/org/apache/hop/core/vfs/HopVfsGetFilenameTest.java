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
package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.local.GenericFileNameParser;
import org.apache.commons.vfs2.provider.local.LocalFileNameParser;
import org.apache.commons.vfs2.provider.local.WindowsFileNameParser;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The four branches of {@link HopVfs#getFilename(FileObject)}, which turns a file object back into
 * something to hand to the operating system.
 *
 * <p>It is worth pinning all four before touching any of them. Around 140 call sites across 65
 * files go through this method and its {@link HopVfs#normalize(String)} wrapper, no test called it
 * directly until this one, and its single biggest caller - the VFS file dialog - is excluded from
 * the coverage report by the {@code **}{@code /*Dialog*} jacoco exclusion. So nothing in the build
 * would notice if a branch changed.
 *
 * <p>Note that the contract is not uniform: two branches return a <em>URI</em> and two return an
 * <em>operating system path</em>. Anything handing the result to {@code new File(...)} therefore
 * behaves differently for a UNC share than for a drive letter.
 */
class HopVfsGetFilenameTest {

  /** A file object is only ever asked for its name, so a name is all a stand-in needs. */
  private static FileObject fileObjectNamed(FileName name) {
    FileObject fileObject = mock(FileObject.class);
    when(fileObject.getName()).thenReturn(name);
    return fileObject;
  }

  private static FileName parse(LocalFileNameParser parser, String uri) throws Exception {
    FileSystemManager fsm = mock(FileSystemManager.class);
    when(fsm.getSchemes()).thenReturn(new String[] {"file"});
    VfsComponentContext context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(fsm);
    return parser.parseUri(context, null, uri);
  }

  /** The separators the caller's platform expects, for the branches returning an OS path. */
  private static String osPath(String slashPath) {
    return "/".equals(Const.FILE_SEPARATOR)
        ? slashPath
        : Const.replace(slashPath, "/", Const.FILE_SEPARATOR);
  }

  @Test
  @DisplayName("a non-file scheme comes back as its URI, untouched")
  void nonFileSchemeReturnsTheUri() {
    FileName name = mock(FileName.class);
    when(name.getRootURI()).thenReturn("sftp://user@host/");
    when(name.getURI()).thenReturn("sftp://user@host/inbox/customers.csv");

    assertEquals("sftp://user@host/inbox/customers.csv", HopVfs.getFilename(fileObjectNamed(name)));
  }

  @Test
  @DisplayName("a UNC share comes back as its URI, all four slashes intact")
  void uncShareReturnsTheUri() throws Exception {
    FileName name = parse(new WindowsFileNameParser(), "file:////server/share/dir/file.txt");

    // Guard the precondition this branch keys off, so a change of root shows up here too.
    assertEquals("file:////server/share/", name.getRootURI());
    assertEquals("file:////server/share/dir/file.txt", HopVfs.getFilename(fileObjectNamed(name)));
  }

  @Test
  @DisplayName("a Windows drive comes back as an OS path, drive letter included")
  void windowsDriveReturnsAnOsPath() throws Exception {
    FileName name = parse(new WindowsFileNameParser(), "file:///C:/temp/file.txt");

    assertEquals("file:///C:/", name.getRootURI());
    assertEquals(osPath("C:/temp/file.txt"), HopVfs.getFilename(fileObjectNamed(name)));
  }

  @Test
  @DisplayName("everywhere else comes back as an OS path with no root prefix")
  void posixReturnsAnOsPath() throws Exception {
    FileName name = parse(new GenericFileNameParser(), "file:///home/me/file.txt");

    assertEquals("file:///", name.getRootURI());
    assertEquals(osPath("/home/me/file.txt"), HopVfs.getFilename(fileObjectNamed(name)));
  }

  /**
   * A UNC-looking name parsed by the generic parser is an ordinary path, so it takes the OS-path
   * branch rather than the URI one. Same input, different answer per platform - which is the thing
   * to keep in mind when changing any of this.
   */
  @Test
  @DisplayName("a UNC-looking name under the generic parser is just a path")
  void uncUnderTheGenericParserIsJustAPath() throws Exception {
    FileName name = parse(new GenericFileNameParser(), "file:////server/share/dir/file.txt");

    assertEquals("file:///", name.getRootURI());
    assertEquals(osPath("/server/share/dir/file.txt"), HopVfs.getFilename(fileObjectNamed(name)));
  }
}
