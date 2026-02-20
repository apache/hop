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
package org.apache.hop.core.fileinput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.commons.vfs2.FileSystemException;
import org.junit.Test;

public class NonAccessibleFileObjectTest {

  @Test
  public void testGetPublicURIString() {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket/mykey");
    assertEquals("s3:///mybucket/mykey", obj.getPublicURIString());
  }

  @Test
  public void testGetPublicURIStringWithNullPath() {
    NonAccessibleFileObject obj = new NonAccessibleFileObject(null);
    assertEquals(null, obj.getPublicURIString());
  }

  @Test
  public void testExists() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.exists());
  }

  @Test
  public void testIsReadable() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.isReadable());
  }

  @Test
  public void testIsWriteable() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.isWriteable());
  }

  @Test
  public void testCanRenameTo() {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.canRenameTo(null));
  }

  @Test
  public void testGetURL() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("http://example.com/file");
    assertEquals("http://example.com/file", obj.getURL().toString());
  }

  @Test(expected = FileSystemException.class)
  public void testGetURLWithInvalidURL() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("not a valid url");
    obj.getURL();
  }

  @Test
  public void testIsAttached() {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.isAttached());
  }

  @Test
  public void testIsContentOpen() {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.isContentOpen());
  }

  @Test
  public void testIsHidden() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    assertFalse(obj.isHidden());
  }

  @Test
  public void testCloseDoesNotThrow() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    obj.close();
  }

  @Test
  public void testRefreshDoesNotThrow() throws FileSystemException {
    NonAccessibleFileObject obj = new NonAccessibleFileObject("s3:///mybucket");
    obj.refresh();
  }
}
