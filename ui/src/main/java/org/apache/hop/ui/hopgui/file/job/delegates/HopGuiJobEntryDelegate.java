package org.apache.hop.ui.hopgui.file.job.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.special.JobEntrySpecial;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.job.entry.JobEntryDialogInterface;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class HopGuiJobEntryDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator2!!


  private HopGui hopUi;
  private HopGuiJobGraph jobGraph;

  public HopGuiJobEntryDelegate( HopGui hopGui, HopGuiJobGraph jobGraph ) {
    this.hopUi = hopGui;
    this.jobGraph = jobGraph;
  }

  public JobEntryCopy newJobEntry( JobMeta jobMeta, String pluginId, String pluginName, boolean openIt, Point location ) {
    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface jobPlugin;

    try {
      if ( pluginId == null ) {
        jobPlugin = PluginRegistry.getInstance().findPluginWithName( JobEntryPluginType.class, pluginName );
      } else {
        jobPlugin = PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, pluginId );
      }
      if ( jobPlugin == null ) {
        // Check if it's not START or DUMMY
        if ( JobMeta.STRING_SPECIAL_START.equalsIgnoreCase( pluginName ) || JobMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( pluginName ) ) {
          jobPlugin = registry.findPluginWithId( JobEntryPluginType.class, JobMeta.STRING_SPECIAL );
        }
      }

      if ( jobPlugin != null ) {
        // Determine name & number for this entry.

        // See if the name is already used...
        //
        String entry_name = pluginName;
        int nr = 2;
        JobEntryCopy check = jobMeta.findJobEntry( entry_name, 0 );
        while ( check != null ) {
          entry_name = pluginName + " " + nr++;
          check = jobMeta.findJobEntry( entry_name, 0 );
        }

        // Generate the appropriate class...
        JobEntryInterface jei = (JobEntryInterface) registry.loadClass( jobPlugin );
        jei.setPluginId( jobPlugin.getIds()[ 0 ] );
        jei.setName( entry_name );

        if ( jei.isSpecial() ) {
          if ( JobMeta.STRING_SPECIAL_START.equalsIgnoreCase( pluginName ) ) {
            // Check if start is already on the canvas...
            if ( jobMeta.findStart() != null ) {
              HopGuiJobGraph.showOnlyStartOnceMessage( hopUi.getShell() );
              return null;
            }
            ( (JobEntrySpecial) jei ).setStart( true );
          }
          if ( JobMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( pluginName ) ) {
            ( (JobEntrySpecial) jei ).setDummy( true );
          }
        }

        if ( openIt ) {
          JobEntryDialogInterface d = getJobEntryDialog( jei, jobMeta );
          if ( d != null && d.open() != null ) {
            JobEntryCopy jge = new JobEntryCopy();
            jge.setEntry( jei );
            if ( location != null ) {
              jge.setLocation( location.x, location.y );
            } else {
              jge.setLocation( 50, 50 );
            }
            jge.setNr( 0 );
            jobMeta.addJobEntry( jge );

            // Verify that the name is not already used in the job.
            //
            jobMeta.renameJobEntryIfNameCollides( jge );

            hopUi.undoDelegate.addUndoNew( jobMeta, new JobEntryCopy[] { jge }, new int[] { jobMeta.indexOfJobEntry( jge ) } );
            jobGraph.updateGui();
            return jge;
          } else {
            return null;
          }
        } else {
          JobEntryCopy jge = new JobEntryCopy();
          jge.setEntry( jei );
          if ( location != null ) {
            jge.setLocation( location.x, location.y );
          } else {
            jge.setLocation( 50, 50 );
          }
          jge.setNr( 0 );
          jobMeta.addJobEntry( jge );
          hopUi.undoDelegate.addUndoNew( jobMeta, new JobEntryCopy[] { jge }, new int[] { jobMeta.indexOfJobEntry( jge ) } );
          jobGraph.updateGui();
          return jge;
        }
      } else {
        return null;
      }
    } catch ( Throwable e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Title" ),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Message" ),
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
      hopUi.getLog().logDebug( "Use of JobEntryInterface#getDialogClassName is deprecated, use PluginDialog annotation instead." );
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
      String errorTitle = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingJobDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingJobEntryDialog.Message", dialogClassName );
      hopUi.getLog().logError( hopUi.toString(), errorMsg );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, t );
      return null;
    }
  }

  public void editJobEntry( JobMeta jobMeta, JobEntryCopy je ) {
    try {
      hopUi.getLog().logBasic(
        hopUi.toString(), BaseMessages.getString( PKG, "HopGui.Log.EditJobEntry", je.getName() ) );

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
          hopUi.undoDelegate.addUndoChange( jobMeta, new JobEntryCopy[] { before }, new JobEntryCopy[] { after }, new int[] { jobMeta.indexOfJobEntry( je ) } );
          jobGraph.updateGui();
        }
      } else {
        MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.JobEntryCanNotBeChanged.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.JobEntryCanNotBeChanged.Title" ) );
        mb.open();
      }

    } catch ( Exception e ) {
      if ( !hopUi.getShell().isDisposed() ) {
        new ErrorDialog( hopUi.getShell(),
          BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorEditingJobEntry.Title" ),
          BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorEditingJobEntry.Message" ), e );
      }
    }
  }

  public void deleteJobEntryCopies( JobMeta job, List<JobEntryCopy> jobEntries ) {

    // Hops belonging to the deleting jobEntries are placed in a single transaction and removed.
    List<JobHopMeta> jobHops = new ArrayList<>();
    int[] hopIndexes = new int[ job.nrJobHops() ];
    int hopIndex = 0;
    for ( int i = job.nrJobHops() - 1; i >= 0; i-- ) {
      JobHopMeta hi = job.getJobHop( i );
      for ( int j = 0; j < jobEntries.size() && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromEntry().equals( jobEntries.get( j ) ) || hi.getToEntry().equals( jobEntries.get( j ) ) ) {
          int idx = job.indexOfJobHop( hi );
          jobHops.add( (JobHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          job.removeJobHop( idx );
          hopIndex++;
          break;
        }
      }
    }
    if ( !jobHops.isEmpty() ) {
      JobHopMeta[] hops = jobHops.toArray( new JobHopMeta[ jobHops.size() ] );
      hopUi.undoDelegate.addUndoDelete( job, hops, hopIndexes );
    }

    // Deleting jobEntries are placed all in a single transaction and removed.
    int[] positions = new int[ jobEntries.size() ];
    for ( int i = 0; i < jobEntries.size(); i++ ) {
      int pos = job.indexOfJobEntry( jobEntries.get( i ) );
      job.removeJobEntry( pos );
      positions[ i ] = pos;
    }
    hopUi.undoDelegate.addUndoDelete( job, jobEntries.toArray( new JobEntryCopy[ 0 ] ), positions );

    jobGraph.updateGui();
  }

  public void deleteJobEntryCopies( JobMeta jobMeta, JobEntryCopy jobEntry ) {

    for ( int i = jobMeta.nrJobHops() - 1; i >= 0; i-- ) {
      JobHopMeta hi = jobMeta.getJobHop( i );
      if ( hi.getFromEntry().equals( jobEntry ) || hi.getToEntry().equals( jobEntry ) ) {
        int idx = jobMeta.indexOfJobHop( hi );
        hopUi.undoDelegate.addUndoDelete( jobMeta, new JobHopMeta[] { (JobHopMeta) hi.clone() }, new int[] { idx } );
        jobMeta.removeJobHop( idx );
      }
    }

    int pos = jobMeta.indexOfJobEntry( jobEntry );
    jobMeta.removeJobEntry( pos );
    hopUi.undoDelegate.addUndoDelete( jobMeta, new JobEntryCopy[] { jobEntry }, new int[] { pos } );

    jobGraph.updateGui();
  }

  public void dupeJobEntry( JobMeta jobMeta, JobEntryCopy jobEntry ) {
    if ( jobEntry == null ) {
      return;
    }

    if ( jobEntry.isStart() ) {
      MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.OnlyUseStartOnce.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.OnlyUseStartOnce.Title" ) );
      mb.open();
      return;
    }

    JobEntryCopy dupejge = (JobEntryCopy) jobEntry.clone();
    dupejge.setNr( jobMeta.findUnusedNr( dupejge.getName() ) );

    Point p = jobEntry.getLocation();
    dupejge.setLocation( p.x + 10, p.y + 10 );

    jobMeta.addJobEntry( dupejge );

    jobGraph.updateGui();
  }


}
