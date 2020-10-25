/*!
 * Hop : The Hop Orchestration Platform
 *
 * Copyright 2019 Hitachi Vantara.  All rights reserved.
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
package org.apache.hop.vfs.s3.s3common;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.vfs2.util.MonitorInputStream;

import java.io.IOException;
import java.io.InputStream;

public class S3CommonFileInputStream extends MonitorInputStream {

  private S3Object s3Object;

  public S3CommonFileInputStream( InputStream in, S3Object s3Object ) {
    super( in );
    this.s3Object = s3Object;
  }

  @Override
  protected void onClose() throws IOException {
    super.onClose();
    this.s3Object.close();
  }
}
