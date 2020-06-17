/*!
 * Copyright 2020 Hitachi Vantara.  All rights reserved.
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

import org.apache.hop.core.config.HopConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that handles operations dealing with hop property file.
 */
public class S3HopProperty {
  public static final String S3VFS_PART_SIZE = "s3.vfs.partSize";

  public String getPartSize() {
    return HopConfig.getSystemProperties().get( S3VFS_PART_SIZE );
  }
}
