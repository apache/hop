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

package org.apache.hop.ui.workflow.actions.special;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.special.ActionSpecial;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;

public class ActionSpecialDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionSpecial.class; // for i18n purposes, needed by Translator!!

  private static final String NOSCHEDULING = BaseMessages.getString( PKG, "JobSpecial.Type.NoScheduling" );

  private static final String INTERVAL = BaseMessages.getString( PKG, "JobSpecial.Type.Interval" );

  private static final String DAILY = BaseMessages.getString( PKG, "JobSpecial.Type.Daily" );

  private static final String WEEKLY = BaseMessages.getString( PKG, "JobSpecial.Type.Weekly" );

  private static final String MONTHLY = BaseMessages.getString( PKG, "JobSpecial.Type.Monthly" );

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private Shell shell;

  private SelectionAdapter lsDef;

  private ActionSpecial action;

  private boolean backupChanged;

  private Display display;

  private Button wRepeat;

  private Spinner wIntervalSeconds, wIntervalMinutes;

  private CCombo wType;

  private Spinner wHour;

  private Spinner wMinutes;

  private CCombo wDayOfWeek;

  private Spinner wDayOfMonth;

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  public ActionSpecialDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionSpecial) action;
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );
    shell.setImage( GuiResource.getInstance().getImageStart() );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        action.setChanged();
      }
    };
    backupChanged = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSpecial.Dummy.Label" ) );

    int margin = props.getMargin();

    int middle = props.getMiddlePct();
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobSpecial.Jobname.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    BaseTransformDialog.setSize( shell, 350, 120, true );
    if ( !this.action.isDummy() ) {
      shell.setText( BaseMessages.getString( PKG, "JobSpecial.Scheduling.Label" ) );
      wRepeat = new Button( shell, SWT.CHECK );
      wRepeat.addListener( SWT.Selection, new Listener() {
        public void handleEvent( Event arg0 ) {
          enableDisableControls();
        }
      } );
      placeControl( shell, BaseMessages.getString( PKG, "JobSpecial.Repeat.Label" ), wRepeat, wName );

      wType = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wType.addModifyListener( lsMod );
      wType.addListener( SWT.Selection, new Listener() {
        public void handleEvent( Event arg0 ) {
          enableDisableControls();
        }
      } );
      wType.add( NOSCHEDULING );
      wType.add( INTERVAL );
      wType.add( DAILY );
      wType.add( WEEKLY );
      wType.add( MONTHLY );
      wType.setEditable( false );
      wType.setVisibleItemCount( wType.getItemCount() );
      placeControl( shell, BaseMessages.getString( PKG, "JobSpecial.Type.Label" ), wType, wRepeat );

      wIntervalSeconds = new Spinner( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wIntervalSeconds.setMinimum( 0 );
      wIntervalSeconds.setMaximum( Integer.MAX_VALUE );
      placeControl(
        shell, BaseMessages.getString( PKG, "JobSpecial.IntervalSeconds.Label" ), wIntervalSeconds, wType );

      wIntervalMinutes = new Spinner( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wIntervalMinutes.setMinimum( 0 );
      wIntervalMinutes.setMaximum( Integer.MAX_VALUE );
      placeControl(
        shell, BaseMessages.getString( PKG, "JobSpecial.IntervalMinutes.Label" ), wIntervalMinutes,
        wIntervalSeconds );

      Composite time = new Composite( shell, SWT.NONE );
      time.setLayout( new FillLayout() );
      wHour = new Spinner( time, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wHour.setMinimum( 0 );
      wHour.setMaximum( 23 );
      wMinutes = new Spinner( time, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wMinutes.setMinimum( 0 );
      wMinutes.setMaximum( 59 );
      placeControl( shell, BaseMessages.getString( PKG, "JobSpecial.TimeOfDay.Label" ), time, wIntervalMinutes );

      wDayOfWeek = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wDayOfWeek.addModifyListener( lsMod );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Sunday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Monday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Tuesday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Wednesday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Thursday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Friday" ) );
      wDayOfWeek.add( BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Saturday" ) );
      wDayOfWeek.setEditable( false );
      wDayOfWeek.setVisibleItemCount( wDayOfWeek.getItemCount() );
      placeControl( shell, BaseMessages.getString( PKG, "JobSpecial.DayOfWeek.Label" ), wDayOfWeek, time );

      wDayOfMonth = new Spinner( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wDayOfMonth.addModifyListener( lsMod );
      wDayOfMonth.setMinimum( 1 );
      wDayOfMonth.setMaximum( 30 );
      placeControl( shell, BaseMessages.getString( PKG, "JobSpecial.DayOfMonth.Label" ), wDayOfMonth, wDayOfWeek );

      lsDef = new SelectionAdapter() {
        public void widgetDefaultSelected( SelectionEvent e ) {
          ok();
        }
      };
      wType.addSelectionListener( lsDef );
      BaseTransformDialog.setSize( shell, 370, 285, true );
    }
    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wDayOfMonth );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );


    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    enableDisableControls();

    shell.open();
    props.setDialogSize( shell, "JobSpecialDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    shell.setSize( 350, 120 );
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void getData() {
    if ( !action.isDummy() ) {
      wRepeat.setSelection( action.isRepeat() );
      wType.select( action.getSchedulerType() );
      wIntervalSeconds.setSelection( action.getIntervalSeconds() );
      wIntervalMinutes.setSelection( action.getIntervalMinutes() );
      wHour.setSelection( action.getHour() );
      wMinutes.setSelection( action.getMinutes() );
      wDayOfWeek.select( action.getWeekDay() );
      wDayOfMonth.setSelection( action.getDayOfMonth() );
      wType.addSelectionListener( lsDef );
    }
    wName.setText( action.getName() );
  }

  private void cancel() {
    action.setChanged( backupChanged );

    action = null;
    dispose();
  }

  private void ok() {
    if ( !action.isDummy() ) {
      action.setRepeat( wRepeat.getSelection() );
      action.setSchedulerType( wType.getSelectionIndex() );
      action.setIntervalSeconds( wIntervalSeconds.getSelection() );
      action.setIntervalMinutes( wIntervalMinutes.getSelection() );
      action.setHour( wHour.getSelection() );
      action.setMinutes( wMinutes.getSelection() );
      action.setWeekDay( wDayOfWeek.getSelectionIndex() );
      action.setDayOfMonth( wDayOfMonth.getSelection() );
    }
    action.setName( wName.getText() );
    dispose();
  }

  private void placeControl( Shell pShell, String text, Control control, Control under ) {
    int middle = props.getMiddlePct();
    int margin = props.getMargin();
    Label label = new Label( pShell, SWT.RIGHT );
    label.setText( text );
    props.setLook( label );
    FormData formDataLabel = new FormData();
    formDataLabel.left = new FormAttachment( 0, 0 );
    if ( under != null ) {
      formDataLabel.top = new FormAttachment( under, margin );
    } else {
      formDataLabel.top = new FormAttachment( 0, 0 );
    }
    formDataLabel.right = new FormAttachment( middle, 0 );
    label.setLayoutData( formDataLabel );

    props.setLook( control );
    FormData formDataControl = new FormData();
    formDataControl.left = new FormAttachment( middle, 0 );
    if ( under != null ) {
      formDataControl.top = new FormAttachment( under, margin );
    } else {
      formDataControl.top = new FormAttachment( 0, 0 );
    }
    formDataControl.right = new FormAttachment( 100, 0 );
    control.setLayoutData( formDataControl );
  }

  private void enableDisableControls() {
    if ( !action.isDummy() ) {
      // if(wRepeat.getSelection()) {
      wType.setEnabled( true );
      if ( NOSCHEDULING.equals( wType.getText() ) ) {
        wIntervalSeconds.setEnabled( false );
        wIntervalMinutes.setEnabled( false );
        wDayOfWeek.setEnabled( false );
        wDayOfMonth.setEnabled( false );
        wHour.setEnabled( false );
        wMinutes.setEnabled( false );
      } else if ( INTERVAL.equals( wType.getText() ) ) {
        wIntervalSeconds.setEnabled( true );
        wIntervalMinutes.setEnabled( true );
        wDayOfWeek.setEnabled( false );
        wDayOfMonth.setEnabled( false );
        wHour.setEnabled( false );
        wMinutes.setEnabled( false );
      } else if ( DAILY.equals( wType.getText() ) ) {
        wIntervalSeconds.setEnabled( false );
        wIntervalMinutes.setEnabled( false );
        wDayOfWeek.setEnabled( false );
        wDayOfMonth.setEnabled( false );
        wHour.setEnabled( true );
        wMinutes.setEnabled( true );
      } else if ( WEEKLY.equals( wType.getText() ) ) {
        wIntervalSeconds.setEnabled( false );
        wIntervalMinutes.setEnabled( false );
        wDayOfWeek.setEnabled( true );
        wDayOfMonth.setEnabled( false );
        wHour.setEnabled( true );
        wMinutes.setEnabled( true );
      } else if ( MONTHLY.equals( wType.getText() ) ) {
        wIntervalSeconds.setEnabled( false );
        wIntervalMinutes.setEnabled( false );
        wDayOfWeek.setEnabled( false );
        wDayOfMonth.setEnabled( true );
        wHour.setEnabled( true );
        wMinutes.setEnabled( true );
      }
      // } else {
      // wType.setEnabled( false );
      // wInterval.setEnabled( false );
      // wDayOfWeek.setEnabled( false );
      // wDayOfMonth.setEnabled( false );
      // wHour.setEnabled( false );
      // wMinutes.setEnabled( false );
    }
  }

}
