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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Props;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.ui.core.PropsUI;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;


public class PreviewRowsDialogTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void getDataForRow() throws Exception {

    IRowMeta iRowMeta = Mockito.mock( IRowMeta.class );
    Mockito.when( iRowMeta.size() ).thenReturn( 3 );
    Mockito.when( iRowMeta.getValueMeta( Mockito.anyInt() ) ).thenReturn( Mockito.mock( IValueMeta.class ) );

    Field propsField = Props.class.getDeclaredField( "props" );
    propsField.setAccessible( true );
    propsField.set( PropsUI.class, Mockito.mock( PropsUI.class ) );

    PreviewRowsDialog previewRowsDialog = new PreviewRowsDialog( Mockito.mock( Shell.class ), Mockito.mock( IVariables.class ), SWT.None, "test",
      iRowMeta, Collections.emptyList() );

    //run without NPE
    int actualResult = previewRowsDialog.getDataForRow( Mockito.mock( TableItem.class ), null );
    Assert.assertEquals( 0, actualResult );
  }
}
