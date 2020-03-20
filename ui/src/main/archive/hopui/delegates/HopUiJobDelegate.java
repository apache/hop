/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui.delegates;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.undo.TransAction;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.special.JobEntrySpecial;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.TabMapEntry;
import org.apache.hop.ui.hopui.TabMapEntry.ObjectType;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.job.dialog.JobExecutionConfigurationDialog;
import org.apache.xul.swt.tab.TabItem;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HopUiJobDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  /**
   * This contains a map between the name of a transformation and the TransMeta object. If the transformation has no
   * name it will be mapped under a number [1], [2] etc.
   */
  private List<JobMeta> jobMap;

  public HopUiJobDelegate( HopUi hopUi ) {
    super( hopUi );
    jobMap = new ArrayList<>();
  }

  public JobEntryCopy newJobEntry( JobMeta jobMeta, String type_desc, boolean openit ) {
    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface jobPlugin;

    try {
      jobPlugin = PluginRegistry.getInstance().findPluginWithName( JobEntryPluginType.class, type_desc );
      if ( jobPlugin == null ) {
        // Check if it's not START or DUMMY
        if ( JobMeta.STRING_SPECIAL_START.equalsIgnoreCase( type_desc ) || JobMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( type_desc ) ) {
          jobPlugin = registry.findPluginWithId( JobEntryPluginType.class, JobMeta.STRING_SPECIAL );
        }
      }

      if ( jobPlugin != null ) {
        // Determine name & number for this entry.

        // See if the name is already used...
        //
        String entry_name = type_desc;
        int nr = 2;
        JobEntryCopy check = jobMeta.findJobEntry( entry_name, 0, true );
        while ( check != null ) {
          entry_name = type_desc + " " + nr++;
          check = jobMeta.findJobEntry( entry_name, 0, true );
        }

        // Generate the appropriate class...
        JobEntryInterface jei = (JobEntryInterface) registry.loadClass( jobPlugin );
        jei.setPluginId( jobPlugin.getIds()[ 0 ] );
        jei.setName( entry_name );

        if ( jei.isSpecial() ) {
          if ( JobMeta.STRING_SPECIAL_START.equalsIgnoreCase( type_desc ) ) {
            // Check if start is already on the canvas...
            if ( jobMeta.findStart() != null ) {
              JobGraph.showOnlyStartOnceMessage( hopUi.getShell() );
              return null;
            }
            ( (JobEntrySpecial) jei ).setStart( true );
          }
          if ( JobMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( type_desc ) ) {
            ( (JobEntrySpecial) jei ).setDummy( true );
          }
        }

        if ( openit ) {
          JobEntryDialogInterface d = getJobEntryDialog( jei, jobMeta );
          if ( d != null && d.open() != null ) {
            JobEntryCopy jge = new JobEntryCopy();
            jge.setEntry( jei );
            jge.setLocation( 50, 50 );
            jge.setNr( 0 );
            jobMeta.addJobEntry( jge );

            // Verify that the name is not already used in the job.
            //
            jobMeta.renameJobEntryIfNameCollides( jge );

            hopUi.addUndoNew( jobMeta, new JobEntryCopy[] { jge }, new int[] { jobMeta.indexOfJobEntry( jge ) } );
            hopUi.refreshGraph();
            hopUi.refreshTree();
            return jge;
          } else {
            return null;
          }
        } else {
          JobEntryCopy jge = new JobEntryCopy();
          jge.setEntry( jei );
          jge.setLocation( 50, 50 );
          jge.setNr( 0 );
          jobMeta.addJobEntry( jge );
          hopUi.addUndoNew( jobMeta, new JobEntryCopy[] { jge }, new int[] { jobMeta.indexOfJobEntry( jge ) } );
          hopUi.refreshGraph();
          hopUi.refreshTree();
          return jge;
        }
      } else {
        return null;
      }
    } catch ( Throwable e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Title" ),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Message" ),
        new Exception( e ) );
      return null;
    }
  }

  public JobEntryDialogInterface getJobEntryDialog( JobEntryInterface jobEntryInterface, JobMeta jobMeta ) {
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, JobEntryInterface.class, JobMeta.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), jobEntryInterface, jobMeta };

    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.getPlugin( JobEntryPluginType.class, jobEntryInterface );
    String dialogClassName = plugin.getClassMap().get( JobEntryDialogInterface.class );
    if ( dialogClassName == null ) {
      // try the deprecated way
      log.logDebug( "Use of JobEntryInterface#getDialogClassName is deprecated, use PluginDialog annotation instead." );
      dialogClassName = jobEntryInterface.getDialogClassName();
    }

    try {
      Class<JobEntryDialogInterface> dialogClass = registry.getClass( plugin, dialogClassName );
      Constructor<JobEntryDialogInterface> dialogConstructor = dialogClass.getConstructor( paramClasses );
      JobEntryDialogInterface entryDialogInterface = dialogConstructor.newInstance( paramArgs );
      entryDialogInterface.setMetaStore( hopUi.getMetaStore() );
      return entryDialogInterface;
    } catch ( Throwable t ) {
      t.printStackTrace();
      String errorTitle = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingJobDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingJobEntryDialog.Message", dialogClassName );
      hopUi.getLog().logError( hopUi.toString(), errorMsg );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, t );
      return null;
    }
  }

  public void editJobEntry( JobMeta jobMeta, JobEntryCopy je ) {
    try {
      hopUi.getLog().logBasic(
        hopUi.toString(), BaseMessages.getString( PKG, "Spoon.Log.EditJobEntry", je.getName() ) );

      JobEntryCopy before = (JobEntryCopy) je.clone_deep();

      JobEntryInterface jei = je.getEntry();

      JobEntryDialogInterface d = getJobEntryDialog( jei, jobMeta );
      if ( d != null ) {
        if ( d.open() != null ) {
          // First see if the name changed.
          // If so, we need to verify that the name is not already used in the job.
          //
          jobMeta.renameJobEntryIfNameCollides( je );

          JobEntryCopy after = (JobEntryCopy) je.clone();
          hopUi.addUndoChange(
            jobMeta, new JobEntryCopy[] { before }, new JobEntryCopy[] { after }, new int[] { jobMeta
              .indexOfJobEntry( je ) } );
          hopUi.refreshGraph();
          hopUi.refreshTree();
        }
      } else {
        MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.JobEntryCanNotBeChanged.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.JobEntryCanNotBeChanged.Title" ) );
        mb.open();
      }

    } catch ( Exception e ) {
      if ( !hopUi.getShell().isDisposed() ) {
        new ErrorDialog( hopUi.getShell(),
          BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorEditingJobEntry.Title" ),
          BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorEditingJobEntry.Message" ), e );
      }
    }
  }

  public void deleteJobEntryCopies( JobMeta job, JobEntryCopy[] jobEntries ) {

    // Hops belonging to the deleting jobEntries are placed in a single transaction and removed.
    List<JobHopMeta> jobHops = new ArrayList<>();
    int[] hopIndexes = new int[ job.nrJobHops() ];
    int hopIndex = 0;
    for ( int i = job.nrJobHops() - 1; i >= 0; i-- ) {
      JobHopMeta hi = job.getJobHop( i );
      for ( int j = 0; j < jobEntries.length && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromEntry().equals( jobEntries[ j ] ) || hi.getToEntry().equals( jobEntries[ j ] ) ) {
          int idx = job.indexOfJobHop( hi );
          jobHops.add( (JobHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          job.removeJobHop( idx );
          hopUi.refreshTree();
          hopIndex++;
          break;
        }
      }
    }
    if ( !jobHops.isEmpty() ) {
      JobHopMeta[] hops = jobHops.toArray( new JobHopMeta[ jobHops.size() ] );
      hopUi.addUndoDelete( job, hops, hopIndexes );
    }

    // Deleting jobEntries are placed all in a single transaction and removed.
    int[] positions = new int[ jobEntries.length ];
    for ( int i = 0; i < jobEntries.length; i++ ) {
      int pos = job.indexOfJobEntry( jobEntries[ i ] );
      job.removeJobEntry( pos );
      positions[ i ] = pos;
    }
    hopUi.addUndoDelete( job, jobEntries, positions );

    hopUi.refreshTree();
    hopUi.refreshGraph();
  }

  public void deleteJobEntryCopies( JobMeta jobMeta, JobEntryCopy jobEntry ) {

    for ( int i = jobMeta.nrJobHops() - 1; i >= 0; i-- ) {
      JobHopMeta hi = jobMeta.getJobHop( i );
      if ( hi.getFromEntry().equals( jobEntry ) || hi.getToEntry().equals( jobEntry ) ) {
        int idx = jobMeta.indexOfJobHop( hi );
        hopUi.addUndoDelete( jobMeta, new JobHopMeta[] { (JobHopMeta) hi.clone() }, new int[] { idx } );
        jobMeta.removeJobHop( idx );
        hopUi.refreshTree();
      }
    }

    int pos = jobMeta.indexOfJobEntry( jobEntry );
    jobMeta.removeJobEntry( pos );
    hopUi.addUndoDelete( jobMeta, new JobEntryCopy[] { jobEntry }, new int[] { pos } );

    hopUi.refreshTree();
    hopUi.refreshGraph();
  }

  public void dupeJobEntry( JobMeta jobMeta, JobEntryCopy jobEntry ) {
    if ( jobEntry == null ) {
      return;
    }

    if ( jobEntry.isStart() ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.OnlyUseStartOnce.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.OnlyUseStartOnce.Title" ) );
      mb.open();
      return;
    }

    JobEntryCopy dupejge = (JobEntryCopy) jobEntry.clone();
    dupejge.setNr( jobMeta.findUnusedNr( dupejge.getName() ) );
    if ( dupejge.isDrawn() ) {
      Point p = jobEntry.getLocation();
      dupejge.setLocation( p.x + 10, p.y + 10 );
    }
    jobMeta.addJobEntry( dupejge );
    hopUi.refreshGraph();
    hopUi.refreshTree();
    hopUi.setShellText();

  }

  public void copyJobEntries( List<JobEntryCopy> jec ) {
    if ( jec == null || jec.size() == 0 ) {
      return;
    }

    StringBuilder xml = new StringBuilder( XMLHandler.getXMLHeader() );
    xml.append( XMLHandler.openTag( HopUi.XML_TAG_JOB_JOB_ENTRIES ) ).append( Const.CR );

    for ( JobEntryCopy aJec : jec ) {
      xml.append( aJec.getXML() );
    }

    xml.append( "    " ).append( XMLHandler.closeTag( HopUi.XML_TAG_JOB_JOB_ENTRIES ) ).append( Const.CR );

    hopUi.toClipboard( xml.toString() );
  }

  public void pasteXML( JobMeta jobMeta, String clipcontent, Point loc ) {
    try {
      Document doc = XMLHandler.loadXMLString( clipcontent );

      // De-select all, re-select pasted steps...
      jobMeta.unselectAll();

      Node entriesnode = XMLHandler.getSubNode( doc, HopUi.XML_TAG_JOB_JOB_ENTRIES );
      int nr = XMLHandler.countNodes( entriesnode, "entry" );
      hopUi.getLog().logDebug( hopUi.toString(), "I found " + nr + " job entries to paste on location: " + loc );
      List<JobEntryCopy> entryList = new ArrayList<>( nr );

      // Point min = new Point(loc.x, loc.y);
      Point min = new Point( 99999999, 99999999 );

      for ( int i = 0; i < nr; i++ ) {
        Node entrynode = XMLHandler.getSubNodeByNr( entriesnode, "entry", i );
        JobEntryCopy copy = new JobEntryCopy( entrynode, hopUi.getMetaStore() );
        if ( copy.isStart() && ( jobMeta.findStart() != null ) ) {
          JobGraph.showOnlyStartOnceMessage( hopUi.getShell() );
          continue;
        }
        String name = jobMeta.getAlternativeJobentryName( copy.getName() );
        copy.setName( name );

        if ( loc != null ) {
          Point p = copy.getLocation();

          if ( min.x > p.x ) {
            min.x = p.x;
          }
          if ( min.y > p.y ) {
            min.y = p.y;
          }
        }

        entryList.add( copy );
      }

      JobEntryCopy[] entries = entryList.toArray( new JobEntryCopy[] {} );

      // What's the difference between loc and min?
      // This is the offset:
      Point offset = new Point( loc.x - min.x, loc.y - min.y );

      // Undo/redo object positions...
      int[] position = new int[ entries.length ];

      for ( int i = 0; i < entries.length; i++ ) {
        Point p = entries[ i ].getLocation();
        String name = entries[ i ].getName();

        entries[ i ].setLocation( p.x + offset.x, p.y + offset.y );

        // Check the name, find alternative...
        entries[ i ].setName( jobMeta.getAlternativeJobentryName( name ) );
        jobMeta.addJobEntry( entries[ i ] );
        position[ i ] = jobMeta.indexOfJobEntry( entries[ i ] );
      }

      // Save undo information too...
      hopUi.addUndoNew( jobMeta, entries, position );

      if ( jobMeta.hasChanged() ) {
        hopUi.refreshTree();
        hopUi.refreshGraph();
      }
    } catch ( HopException e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorPasingJobEntries.Title" ),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorPasingJobEntries.Message" ), e );
    }
  }

  public void newJobHop( JobMeta jobMeta, JobEntryCopy fr, JobEntryCopy to ) {
    JobHopMeta hi = new JobHopMeta( fr, to );
    jobMeta.addJobHop( hi );
    hopUi.addUndoNew( jobMeta, new JobHopMeta[] { hi }, new int[] { jobMeta.indexOfJobHop( hi ) } );
    hopUi.refreshGraph();
    hopUi.refreshTree();
  }

  public boolean isDefaultJobName( String name ) {
    if ( !name.startsWith( HopUi.STRING_JOB ) ) {
      return false;
    }

    // see if there are only digits behind the job...
    // This will detect:
    // "Job"
    // "Job "
    // "Job 1"
    // "Job 2"
    // ...
    for ( int i = HopUi.STRING_JOB.length() + 1; i < name.length(); i++ ) {
      if ( !Character.isDigit( name.charAt( i ) ) ) {
        return false;
      }
    }
    return true;
  }

  public JobGraph findJobGraphOfJob( JobMeta jobMeta ) {
    // Now loop over the entries in the tab-map
    for ( TabMapEntry mapEntry : hopUi.delegates.tabs.getTabs() ) {
      if ( mapEntry.getObject() instanceof JobGraph ) {
        JobGraph jobGraph = (JobGraph) mapEntry.getObject();
        if ( jobGraph.getMeta().equals( jobMeta ) ) {
          return jobGraph;
        }
      }
    }
    return null;
  }

  /**
   * Add a job to the job map
   *
   * @param jobMeta the job to add to the map
   * @return true if the job was added
   */
  public boolean addJob( JobMeta jobMeta ) {
    int index = getJobList().indexOf( jobMeta );
    if ( index < 0 ) {
      getJobList().add( jobMeta );
      return true;
    } else {
      /*
       * ShowMessageDialog dialog = new ShowMessageDialog(spoon.getShell(), SWT.OK | SWT.ICON_INFORMATION,
       * BaseMessages.getString(PKG, "Spoon.Dialog.JobAlreadyLoaded.Title"), "'" + jobMeta.toString() + "'" + Const.CR +
       * Const.CR + BaseMessages.getString(PKG, "Spoon.Dialog.JobAlreadyLoaded.Message")); dialog.setTimeOut(6);
       * dialog.open();
       */
      return false;
    }

  }

  /**
   * @param jobMeta the transformation to close, make sure it's ok to dispose of it BEFORE you call this.
   */
  public void closeJob( JobMeta jobMeta ) {
    // Close the associated tabs...
    //
    TabMapEntry entry = getSpoon().delegates.tabs.findTabMapEntry( jobMeta );
    if ( entry != null ) {
      getSpoon().delegates.tabs.removeTab( entry );
    }

    // Also remove it from the item from the jobMap
    // Otherwise it keeps showing up in the objects tree
    //
    int index = getJobList().indexOf( jobMeta );
    while ( index >= 0 ) {
      getJobList().remove( index );
      index = getJobList().indexOf( jobMeta );
    }

    getSpoon().refreshTree();
    getSpoon().enableMenus();
  }

  protected HopUi getSpoon() {
    return this.hopUi;
  }

  public void addJobGraph( JobMeta jobMeta ) {
    boolean added = addJob( jobMeta );
    if ( added ) {
      // See if there already is a tab for this graph with the short default name.
      // If there is, set that one to show the location as well.
      // If not, simply add it without
      // If no, add it
      // If yes, select that tab
      //
      boolean showLocation = false;
      boolean addTab = true;
      String tabName = hopUi.delegates.tabs.makeTabName( jobMeta, false );

      TabMapEntry tabEntry = hopUi.delegates.tabs.findTabMapEntry( tabName, ObjectType.JOB_GRAPH );
      if ( tabEntry != null ) {
        // We change the already loaded job to also show the location.
        //
        showLocation = true;
        tabEntry.setShowingLocation( true );
        String newTabName = hopUi.delegates.tabs.makeTabName( tabEntry.getObject().getMeta(), true );
        tabEntry.getTabItem().setText( newTabName );

        // Try again, including the location of the object...
        //
        tabName = hopUi.delegates.tabs.makeTabName( jobMeta, true );
        tabEntry = hopUi.delegates.tabs.findTabMapEntry( tabName, ObjectType.JOB_GRAPH );
        if ( tabEntry != null ) {
          // Already loaded, simply select the tab item in question...
          //
          addTab = false;
        }
      }

      if ( addTab ) {
        JobGraph jobGraph = new JobGraph( hopUi.tabfolder.getSwtTabset(), hopUi, jobMeta );

        PropsUI props = PropsUI.getInstance();

        if ( tabName.length() >= getMaxTabLength() ) {
          tabName = new StringBuilder().append( tabName.substring( 0, getMaxTabLength() ) ).append( "\u2026" ).toString();
        }
        TabItem tabItem = new TabItem( hopUi.tabfolder, tabName, tabName, props.getSashWeights() );
        String toolTipText =
          BaseMessages.getString( PKG, "Spoon.TabJob.Tooltip", hopUi.delegates.tabs.makeTabName(
            jobMeta, showLocation ) );
        if ( !Utils.isEmpty( jobMeta.getFilename() ) ) {
          toolTipText += Const.CR + Const.CR + jobMeta.getFilename();
        }
        tabItem.setToolTipText( toolTipText );
        tabItem.setImage( GUIResource.getInstance().getImageJobGraph() );
        tabItem.setControl( jobGraph );

        tabEntry = new TabMapEntry( tabItem, jobMeta.getFilename(), jobMeta.getName(), jobGraph, ObjectType.JOB_GRAPH );
        tabEntry.setShowingLocation( showLocation );

        hopUi.delegates.tabs.addTab( tabEntry );
      }

      int idx = hopUi.tabfolder.indexOf( tabEntry.getTabItem() );

      // keep the focus on the graph
      hopUi.tabfolder.setSelected( idx );

      hopUi.setUndoMenu( jobMeta );
      hopUi.enableMenus();
    } else {
      TabMapEntry tabEntry = hopUi.delegates.tabs.findTabMapEntry( jobMeta );

      if ( tabEntry != null ) {
        int idx = hopUi.tabfolder.indexOf( tabEntry.getTabItem() );

        // keep the focus on the graph
        hopUi.tabfolder.setSelected( idx );

        // keep the focus on the graph
        hopUi.tabfolder.setSelected( idx );
        hopUi.setUndoMenu( jobMeta );
        hopUi.enableMenus();
      }
    }
  }

  /*
   * private void addJobLog(JobMeta jobMeta) { // See if there already is a tab for this log // If no, add it // If yes,
   * select that tab // String tabName = spoon.delegates.tabs.makeJobLogTabName(jobMeta); TabItem tabItem =
   * spoon.delegates.tabs.findTabItem(tabName, TabMapEntry.OBJECT_TYPE_JOB_LOG); if (tabItem == null) { JobLog jobLog =
   * new JobLog(spoon.tabfolder.getSwtTabset(), spoon, jobMeta); tabItem = new TabItem(spoon.tabfolder, tabName,
   * tabName); tabItem.setText(tabName); tabItem.setToolTipText(BaseMessages.getString(PKG,
   * "Spoon.Title.ExecLogJobView.Tooltip", spoon.delegates.tabs .makeJobGraphTabName(jobMeta)));
   * tabItem.setControl(jobLog);
   *
   * // If there is an associated history window, we want to keep that // one up-to-date as well. // JobHistory
   * jobHistory = findJobHistoryOfJob(jobMeta); TabItem historyItem =
   * spoon.delegates.tabs.findTabItem(spoon.delegates.tabs.makeJobHistoryTabName(jobMeta),
   * TabMapEntry.OBJECT_TYPE_JOB_HISTORY);
   *
   * if (jobHistory != null && historyItem != null) { JobHistoryRefresher jobHistoryRefresher = new
   * JobHistoryRefresher(historyItem, jobHistory); spoon.tabfolder.addListener(jobHistoryRefresher); //
   * jobLog.setJobHistoryRefresher(jobHistoryRefresher); }
   *
   * spoon.delegates.tabs.addTab(new TabMapEntry(tabItem, tabName, jobLog, TabMapEntry.OBJECT_TYPE_JOB_LOG)); } int idx
   * = spoon.tabfolder.indexOf(tabItem); spoon.tabfolder.setSelected(idx); }
   */

  public List<JobMeta> getJobList() {
    return jobMap;
  }

  public JobMeta getJob( String name ) {
    TabMapEntry entry = hopUi.delegates.tabs.findTabMapEntry( name, ObjectType.JOB_GRAPH );
    if ( entry != null ) {
      return (JobMeta) entry.getObject().getManagedObject();
    }

    // TODO: remove part below
    //
    for ( JobMeta jobMeta : jobMap ) {
      if ( name != null && name.equals( jobMeta.getName() ) ) {
        return jobMeta;
      }
    }
    return null;
  }

  public JobMeta[] getLoadedJobs() {
    return jobMap.toArray( new JobMeta[ jobMap.size() ] );
  }

  public void redoJobAction( JobMeta jobMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      //
      // NEW
      //
      case NewJobEntry:
        // re-delete the entry at correct location:
        JobEntryCopy[] si = (JobEntryCopy[]) transAction.getCurrent();
        int[] idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.addJobEntry( idx[ i ], si[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case NewNote:
        // re-insert the note at correct location:
        NotePadMeta[] ni = (NotePadMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.addNote( idx[ i ], ni[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case NewJobHop:
        // re-insert the hop at correct location:
        JobHopMeta[] hi = (JobHopMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.addJobHop( idx[ i ], hi[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // DELETE
      //
      case DeleteJobEntry:
        // re-remove the entry at correct location:
        idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeJobEntry( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case DeleteNote:
        // re-remove the note at correct location:
        idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeNote( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case DeleteJobHop:
        // re-remove the hop at correct location:
        idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeJobHop( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case ChangeJobEntry:
        // replace with "current" version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          JobEntryCopy copy = (JobEntryCopy) ( (JobEntryCopy) ( transAction.getCurrent()[ i ] ) ).clone_deep();
          jobMeta.getJobEntry( transAction.getCurrentIndex()[ i ] ).replaceMeta( copy );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        ni = (NotePadMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();

        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.removeNote( idx[ i ] );
          jobMeta.addNote( idx[ i ], ni[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a hop : undo this...
      case ChangeJobHop:
        // Delete & re-insert
        hi = (JobHopMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();

        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.removeJobHop( idx[ i ] );
          jobMeta.addJobHop( idx[ i ], hi[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE POSITION
      //
      case PositionJobEntry:
        // Find the location of the step:
        idx = transAction.getCurrentIndex();
        Point[] p = transAction.getCurrentLocation();
        for ( int i = 0; i < p.length; i++ ) {
          JobEntryCopy entry = jobMeta.getJobEntry( idx[ i ] );
          entry.setLocation( p[ i ] );
        }
        hopUi.refreshGraph();
        break;

      case PositionNote:
        idx = transAction.getCurrentIndex();
        Point[] curr = transAction.getCurrentLocation();
        for ( int i = 0; i < idx.length; i++ ) {
          NotePadMeta npi = jobMeta.getNote( idx[ i ] );
          npi.setLocation( curr[ i ] );
        }
        hopUi.refreshGraph();
        break;

      default:
        break;
    }
  }

  public void undoJobAction( JobMeta jobMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      // We created a new entry : undo this...
      case NewJobEntry:
        // Delete the entry at correct location:
        int[] idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeJobEntry( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new note : undo this...
      case NewNote:
        // Delete the note at correct location:
        idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeNote( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new hop : undo this...
      case NewJobHop:
        // Delete the hop at correct location:
        idx = transAction.getCurrentIndex();
        for ( int i = idx.length - 1; i >= 0; i-- ) {
          jobMeta.removeJobHop( idx[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // DELETE
      //

      // We delete an entry : undo this...
      case DeleteJobEntry:
        // un-Delete the entry at correct location: re-insert
        JobEntryCopy[] ce = (JobEntryCopy[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < ce.length; i++ ) {
          jobMeta.addJobEntry( idx[ i ], ce[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We delete new note : undo this...
      case DeleteNote:
        // re-insert the note at correct location:
        NotePadMeta[] ni = (NotePadMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.addNote( idx[ i ], ni[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We deleted a new hop : undo this...
      case DeleteJobHop:
        // re-insert the hop at correct location:
        JobHopMeta[] hi = (JobHopMeta[]) transAction.getCurrent();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < hi.length; i++ ) {
          jobMeta.addJobHop( idx[ i ], hi[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE
      //

      // We changed a job entry: undo this...
      case ChangeJobEntry:
        // Delete the current job entry, insert previous version.
        for ( int i = 0; i < transAction.getPrevious().length; i++ ) {
          JobEntryCopy copy = (JobEntryCopy) ( (JobEntryCopy) transAction.getPrevious()[ i ] ).clone();
          jobMeta.getJobEntry( transAction.getCurrentIndex()[ i ] ).replaceMeta( copy );

        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a note : undo this...
      case ChangeNote:
        // Delete & re-insert
        NotePadMeta[] prev = (NotePadMeta[]) transAction.getPrevious();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.removeNote( idx[ i ] );
          jobMeta.addNote( idx[ i ], prev[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a hop : undo this...
      case ChangeJobHop:
        // Delete & re-insert
        JobHopMeta[] prevHops = (JobHopMeta[]) transAction.getPrevious();
        idx = transAction.getCurrentIndex();
        for ( int i = 0; i < idx.length; i++ ) {
          jobMeta.removeJobHop( idx[ i ] );
          jobMeta.addJobHop( idx[ i ], prevHops[ i ] );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // POSITION
      //

      // The position of a step has changed: undo this...
      case PositionJobEntry:
        // Find the location of the step:
        idx = transAction.getCurrentIndex();
        Point[] p = transAction.getPreviousLocation();
        for ( int i = 0; i < p.length; i++ ) {
          JobEntryCopy entry = jobMeta.getJobEntry( idx[ i ] );
          entry.setLocation( p[ i ] );
        }
        hopUi.refreshGraph();
        break;

      // The position of a note has changed: undo this...
      case PositionNote:
        idx = transAction.getCurrentIndex();
        Point[] prevLoc = transAction.getPreviousLocation();
        for ( int i = 0; i < idx.length; i++ ) {
          NotePadMeta npi = jobMeta.getNote( idx[ i ] );
          npi.setLocation( prevLoc[ i ] );
        }
        hopUi.refreshGraph();
        break;

      default:
        break;
    }
  }

  public void executeJob( JobMeta jobMeta, boolean local, boolean remote, boolean safe, String startCopyName, int startCopyNr ) throws HopException {

    if ( jobMeta == null ) {
      return;
    }

    JobExecutionConfiguration executionConfiguration = hopUi.getJobExecutionConfiguration();

    // Remember the variables set previously
    //
    Object[] data = hopUi.variables.getData();
    String[] fields = hopUi.variables.getRowMeta().getFieldNames();
    Map<String, String> variableMap = new HashMap<>();
    for ( int idx = 0; idx < fields.length; idx++ ) {
      variableMap.put( fields[ idx ], data[ idx ].toString() );
    }

    executionConfiguration.setVariables( variableMap );
    executionConfiguration.getUsedVariables( jobMeta );
    executionConfiguration.setSafeModeEnabled( safe );
    executionConfiguration.setStartCopyName( startCopyName );
    executionConfiguration.setStartCopyNr( startCopyNr );
    executionConfiguration.setLogLevel( DefaultLogLevel.getLogLevel() );

    JobExecutionConfigurationDialog dialog = newJobExecutionConfigurationDialog( executionConfiguration, jobMeta );

    if ( !jobMeta.isShowDialog() || dialog.open() ) {

      JobGraph jobGraph = hopUi.getActiveJobGraph();
      jobGraph.jobLogDelegate.addJobLog();

      // Set the variables that where specified...
      //
      for ( String varName : executionConfiguration.getVariables().keySet() ) {
        String varValue = executionConfiguration.getVariables().get( varName );
        jobMeta.setVariable( varName, varValue );
      }

      // Set and activate the parameters...
      //
      for ( String paramName : executionConfiguration.getParams().keySet() ) {
        String paramValue = executionConfiguration.getParams().get( paramName );
        jobMeta.setParameterValue( paramName, paramValue );
      }
      jobMeta.activateParameters();

      // Set the log level
      //
      if ( executionConfiguration.getLogLevel() != null ) {
        jobMeta.setLogLevel( executionConfiguration.getLogLevel() );
      }

      // Set the start step name
      //
      if ( executionConfiguration.getStartCopyName() != null ) {
        jobMeta.setStartCopyName( executionConfiguration.getStartCopyName() );
      }

      // Set the run options
      //
      jobMeta.setClearingLog( executionConfiguration.isClearingLog() );
      jobMeta.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
      jobMeta.setExpandingRemoteJob( executionConfiguration.isExpandingRemoteJob() );

      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobMetaExecutionStart.id, jobMeta );
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobExecutionConfiguration.id,
        executionConfiguration );

      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransBeforeStart.id, new Object[] {
          executionConfiguration, jobMeta, jobMeta } );
      } catch ( HopException e ) {
        log.logError( e.getMessage(), jobMeta.getFilename() );
        return;
      }

      if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() ) {
        if ( jobMeta.hasChanged() ) {
          jobGraph.showSaveFileMessage();
        }
      }

      // Is this a local execution?
      //
      if ( executionConfiguration.isExecutingLocally() ) {
        jobGraph.startJob( executionConfiguration );
      } else if ( executionConfiguration.isExecutingRemotely() ) {
        // Executing remotely
        // Check if jobMeta has changed
        jobGraph.handleJobMetaChanges( jobMeta );

        // Activate the parameters, turn them into variables...
        // jobMeta.hasChanged()
        jobMeta.activateParameters();

        if ( executionConfiguration.getRemoteServer() != null ) {
          Job.sendToSlaveServer( jobMeta, executionConfiguration, hopUi.metaStore );
          hopUi.delegates.slaves.addSpoonSlave( executionConfiguration.getRemoteServer() );
        } else {
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.NoRemoteServerSpecified.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.NoRemoteServerSpecified.Title" ) );
          mb.open();
        }
      }
    }
  }

  @VisibleForTesting
  JobExecutionConfigurationDialog newJobExecutionConfigurationDialog( JobExecutionConfiguration executionConfiguration, JobMeta jobMeta ) {
    return new JobExecutionConfigurationDialog( hopUi.getShell(), executionConfiguration, jobMeta );
  }
}
