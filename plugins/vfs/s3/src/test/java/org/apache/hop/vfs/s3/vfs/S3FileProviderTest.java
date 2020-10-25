/*!
* Hop : The Hop Orchestration Platform
 *
 * Copyright 2010 - 2019 Hitachi Vantara.  All rights reserved.
 * http://www.project-hop.org
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package org.apache.hop.vfs.s3.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


/**
 * Unit tests for S3FileProvider
 */
public class S3FileProviderTest {

  S3FileProvider provider;

  @Before
  public void setUp() throws Exception {
    provider = new S3FileProvider();
  }

  @Test
  public void testDoCreateFileSystem() throws Exception {
    FileName fileName = mock( FileName.class );
    FileSystemOptions options = new FileSystemOptions();
    assertNotNull( provider.doCreateFileSystem( fileName, options ) );
  }

}
