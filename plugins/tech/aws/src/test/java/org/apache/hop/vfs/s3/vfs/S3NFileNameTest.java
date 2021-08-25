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

package org.apache.hop.vfs.s3.vfs;

import org.apache.commons.vfs2.FileType;
import org.apache.hop.vfs.s3.s3n.vfs.S3NFileName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** created by: rfellows date: 05/17/2012 */
public class S3NFileNameTest {

  private S3NFileName fileName = null;

  public static final String HOST = "S3";
  public static final String SCHEME = "s3n";
  public static final int PORT = 843;

  @BeforeClass
  public static void init() throws Exception {}

  @Before
  public void setup() {
    fileName = new S3NFileName(SCHEME, "", "", FileType.FOLDER);
  }

  @Test
  public void testGetURI() throws Exception {
    String expected = buildS3URL("/");
    assertEquals(expected, fileName.getURI());
  }

  @Test
  public void testCreateName() {
    assertEquals(
        "s3n:///path/to/my/file", fileName.createName("/path/to/my/file", FileType.FILE).getURI());
  }

  @Test
  public void testAppendRootUriWithNonDefaultPort() {
    fileName = new S3NFileName(SCHEME, "", "FooFolder", FileType.FOLDER);
    String expectedUri = SCHEME + "://" + "FooFolder";
    assertEquals(expectedUri, fileName.getURI());

    fileName = new S3NFileName(SCHEME, "FooBucket", "/FooBucket/FooFolder", FileType.FOLDER);
    expectedUri = SCHEME + "://FooBucket/FooBucket/" + "FooFolder";
    assertEquals(expectedUri, fileName.getURI());
  }

  public static String buildS3URL(String path) {
    return SCHEME + "://" + path;
  }
}
