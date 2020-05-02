/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.sftpput;

import org.apache.hop.workflow.actions.sftp.SFTPClient;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Send file to SFTP host.
 *
 * @author Samatar Hassan
 * @since 30-April-2012
 */
public class SftpPutData extends BaseTransformData implements ITransformData {

  // SFTP connection
  public SFTPClient sftpclient;
  // Index Of sourcefilename field
  public int indexOfSourceFileFieldName;
  // index of remote directory
  public int indexOfRemoteDirectory;
  // Index of movetofolder
  public int indexOfMoveToFolderFieldName;
  // index of remote filename
  public int indexOfRemoteFilename;

  public SftpPutData() {
    super();
    this.indexOfSourceFileFieldName = -1;
    this.indexOfRemoteDirectory = -1;
    this.indexOfMoveToFolderFieldName = -1;
    this.indexOfRemoteFilename = -1;
  }

}
