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

package org.apache.hop.ui.pipeline.transforms.fileinput.text;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.errorhandling.FileErrorHandler;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileFilter;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileFilterProcessor;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInput;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputData;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.widgets.Shell;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by jadametz on 9/9/15.
 */
public class TextFileInputDialogTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static boolean changedPropsUi;

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeClass
  public static void hackPropsUi() throws Exception {
    Field props = getPropsField();
    if ( props == null ) {
      throw new IllegalStateException( "Cannot find 'props' field in " + Props.class.getName() );
    }

    Object value = FieldUtils.readStaticField( props, true );
    if ( value == null ) {
      PropsUI mock = mock( PropsUI.class );
      FieldUtils.writeStaticField( props, mock, true );
      changedPropsUi = true;
    } else {
      changedPropsUi = false;
    }
  }

  @AfterClass
  public static void restoreNullInPropsUi() throws Exception {
    if ( changedPropsUi ) {
      Field props = getPropsField();
      FieldUtils.writeStaticField( props, null, true );
    }
  }

  private static Field getPropsField() {
    return FieldUtils.getDeclaredField( Props.class, "props", true );
  }

  @Test
  public void testMinimalWidth_PDI_14253() throws Exception {
    final String virtualFile = "ram://pdi-14253.txt";
    HopVFS.getFileObject( virtualFile ).createFile();

    final String content = "r1c1,  r1c2\nr2c1  ,  r2c2  ";
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write( content.getBytes() );

    OutputStream os = HopVFS.getFileObject( virtualFile ).getContent().getOutputStream();
    IOUtils.copy( new ByteArrayInputStream( bos.toByteArray() ), os );
    os.close();

    TextFileInputMeta meta = new TextFileInputMeta();
    meta.content.lineWrapped = false;
    meta.inputFields = new BaseFileField[] {
      new BaseFileField( "col1", -1, -1 ),
      new BaseFileField( "col2", -1, -1 )
    };
    meta.content.fileCompression = "None";
    meta.content.fileType = "CSV";
    meta.content.header = false;
    meta.content.nrHeaderLines = -1;
    meta.content.footer = false;
    meta.content.nrFooterLines = -1;

    TextFileInputData data = new TextFileInputData();
    data.files = new FileInputList();
    data.files.addFile( HopVFS.getFileObject( virtualFile ) );

    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col1" ) );
    data.outputRowMeta.addValueMeta( new ValueMetaString( "col2" ) );

    data.dataErrorLineHandler = mock( FileErrorHandler.class );
    data.fileFormatType = TextFileInputMeta.FILE_FORMAT_UNIX;
    data.separator = ",";
    data.filterProcessor = new TextFileFilterProcessor( new TextFileFilter[ 0 ], new Variables() {
    } );
    data.filePlayList = new FilePlayListAll();

    TextFileInputDialog dialog =
      new TextFileInputDialog( mock( Shell.class ), meta, mock( PipelineMeta.class ), "TFIMinimalWidthTest" );
    TableView tv = mock( TableView.class );
    when( tv.nrNonEmpty() ).thenReturn( 0 );

    // click the Minimal width button
    dialog.setMinimalWidth( tv );

    RowSet output = new BlockingRowSet( 5 );
    TextFileInput input = TransformMockUtil.getTransform( TextFileInput.class, TextFileInputMeta.class, "test" );
    input.setOutputRowSets( Collections.singletonList( output ) );
    while ( input.processRow() ) {
      // wait until the transform completes executing
    }

    Object[] row1 = output.getRowImmediate();
    assertRow( row1, "r1c1", "r1c2" );

    Object[] row2 = output.getRowImmediate();
    assertRow( row2, "r2c1", "r2c2" );

    HopVFS.getFileObject( virtualFile ).delete();

  }

  private static void assertRow( Object[] row, Object... values ) {
    assertNotNull( row );
    assertTrue( String.format( "%d < %d", row.length, values.length ), row.length >= values.length );
    int i = 0;
    while ( i < values.length ) {
      assertEquals( values[ i ], row[ i ] );
      i++;
    }
    while ( i < row.length ) {
      assertNull( row[ i ] );
      i++;
    }
  }
}
