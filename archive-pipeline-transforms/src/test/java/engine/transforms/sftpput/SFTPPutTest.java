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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.actions.sftp.SFTPClient;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Andrey Khayrutdinov
 */
public class SFTPPutTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private SFTPPut transform;

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {
    SFTPClient clientMock = mock( SFTPClient.class );

    transform = TransformMockUtil.getTransform( SFTPPut.class, SFTPPutMeta.class, "mock transform" );
    transform = spy( transform );
    doReturn( clientMock ).when( transform )
      .createSftpClient( anyString(), anyString(), anyString(), anyString(), anyString() );
  }


  private static RowMeta rowOfStringsMeta( String... columns ) {
    RowMeta rowMeta = new RowMeta();
    for ( String column : columns ) {
      rowMeta.addValueMeta( new ValueMetaString( column ) );
    }
    return rowMeta;
  }


  @Test
  public void checkRemoteFilenameField_FieldNameIsBlank() throws Exception {
    SFTPPutData data = new SFTPPutData();
    transform.checkRemoteFilenameField( "", data );
    assertEquals( -1, data.indexOfSourceFileFieldName );
  }

  @Test( expected = HopTransformException.class )
  public void checkRemoteFilenameField_FieldNameIsSet_NotFound() throws Exception {
    transform.setInputRowMeta( new RowMeta() );
    transform.checkRemoteFilenameField( "remoteFileName", new SFTPPutData() );
  }

  @Test
  public void checkRemoteFilenameField_FieldNameIsSet_Found() throws Exception {
    RowMeta rowMeta = rowOfStringsMeta( "some field", "remoteFileName" );
    transform.setInputRowMeta( rowMeta );

    SFTPPutData data = new SFTPPutData();
    transform.checkRemoteFilenameField( "remoteFileName", data );
    assertEquals( 1, data.indexOfRemoteFilename );
  }


  @Test( expected = HopTransformException.class )
  public void checkSourceFileField_NameIsBlank() throws Exception {
    SFTPPutData data = new SFTPPutData();
    transform.checkSourceFileField( "", data );
  }

  @Test( expected = HopTransformException.class )
  public void checkSourceFileField_NameIsSet_NotFound() throws Exception {
    transform.setInputRowMeta( new RowMeta() );
    transform.checkSourceFileField( "sourceFile", new SFTPPutData() );
  }

  @Test
  public void checkSourceFileField_NameIsSet_Found() throws Exception {
    RowMeta rowMeta = rowOfStringsMeta( "some field", "sourceFileFieldName" );
    transform.setInputRowMeta( rowMeta );

    SFTPPutData data = new SFTPPutData();
    transform.checkSourceFileField( "sourceFileFieldName", data );
    assertEquals( 1, data.indexOfSourceFileFieldName );
  }


  @Test( expected = HopTransformException.class )
  public void checkRemoteFoldernameField_NameIsBlank() throws Exception {
    SFTPPutData data = new SFTPPutData();
    transform.checkRemoteFoldernameField( "", data );
  }

  @Test( expected = HopTransformException.class )
  public void checkRemoteFoldernameField_NameIsSet_NotFound() throws Exception {
    transform.setInputRowMeta( new RowMeta() );
    transform.checkRemoteFoldernameField( "remoteFolder", new SFTPPutData() );
  }

  @Test
  public void checkRemoteFoldernameField_NameIsSet_Found() throws Exception {
    RowMeta rowMeta = rowOfStringsMeta( "some field", "remoteFoldernameFieldName" );
    transform.setInputRowMeta( rowMeta );

    SFTPPutData data = new SFTPPutData();
    transform.checkRemoteFoldernameField( "remoteFoldernameFieldName", data );
    assertEquals( 1, data.indexOfRemoteDirectory );
  }


  @Test( expected = HopTransformException.class )
  public void checkDestinationFolderField_NameIsBlank() throws Exception {
    SFTPPutData data = new SFTPPutData();
    transform.checkDestinationFolderField( "", data );
  }

  @Test( expected = HopTransformException.class )
  public void checkDestinationFolderField_NameIsSet_NotFound() throws Exception {
    transform.setInputRowMeta( new RowMeta() );
    transform.checkDestinationFolderField( "destinationFolder", new SFTPPutData() );
  }

  @Test
  public void checkDestinationFolderField_NameIsSet_Found() throws Exception {
    RowMeta rowMeta = rowOfStringsMeta( "some field", "destinationFolderFieldName" );
    transform.setInputRowMeta( rowMeta );

    SFTPPutData data = new SFTPPutData();
    transform.checkDestinationFolderField( "destinationFolderFieldName", data );
    assertEquals( 1, data.indexOfMoveToFolderFieldName );
  }


  @Test
  public void remoteFilenameFieldIsMandatoryWhenStreamingFromInputField() throws Exception {
    RowMeta rowMeta = rowOfStringsMeta( "sourceFilenameFieldName", "remoteDirectoryFieldName" );
    transform.setInputRowMeta( rowMeta );

    doReturn( new Object[] { "qwerty", "asdfg" } ).when( transform ).getRow();

    SFTPPutMeta meta = new SFTPPutMeta();
    meta.setInputStream( true );
    meta.setPassword( "qwerty" );
    meta.setSourceFileFieldName( "sourceFilenameFieldName" );
    meta.setRemoteDirectoryFieldName( "remoteDirectoryFieldName" );

    transform.processRow();
    assertEquals( 1, transform.getErrors() );
  }
}
