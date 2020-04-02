package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Partitioner;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformDialogInterface;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.IPartitionSchemaSelection;
import org.apache.hop.ui.hopgui.partition.PartitionMethodSelector;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.apache.hop.ui.hopgui.partition.processor.MethodProcessor;
import org.apache.hop.ui.hopgui.partition.processor.MethodProcessorFactory;
import org.apache.hop.ui.pipeline.transform.TransformErrorMetaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineTransformDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!


  private HopGui hopUi;
  private HopGuiPipelineGraph pipelineGraph;

  public HopGuiPipelineTransformDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopUi = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public TransformDialogInterface getTransformDialog( TransformMetaInterface transformMeta, PipelineMeta pipelineMeta, String transformName ) throws HopException {
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, Object.class, PipelineMeta.class, String.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), transformMeta, pipelineMeta, transformName };

    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.getPlugin( TransformPluginType.class, transformMeta );
    String dialogClassName = plugin.getClassMap().get( TransformDialogInterface.class );
    if ( dialogClassName == null ) {
      // Calculate it from the base meta class...
      //
      dialogClassName = transformMeta.getDialogClassName();
    }

    if ( dialogClassName == null ) {
      throw new HopException( "Unable to find dialog class for plugin '" + plugin.getIds()[ 0 ] + "' : " + plugin.getName() );
    }

    try {
      Class<TransformDialogInterface> dialogClass = registry.getClass( plugin, dialogClassName );
      Constructor<TransformDialogInterface> dialogConstructor = dialogClass.getConstructor( paramClasses );
      return dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      // try the old way for compatibility
      try {
        Class<?>[] sig = new Class<?>[] { Shell.class, TransformMetaInterface.class, PipelineMeta.class, String.class };
        Method method = transformMeta.getClass().getDeclaredMethod( "getDialog", sig );
        if ( method != null ) {
          hopUi.getLog().logDebug( "Use of TransformMetaInterface#getDialog is deprecated, use PluginDialog annotation instead." );
          return (TransformDialogInterface) method.invoke( transformMeta, paramArgs );
        }
      } catch ( Throwable ignored ) {
      }

      String errorTitle = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingTransformDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingTransformDialog.Message", dialogClassName );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, e );
      throw new HopException( e );
    }
  }

  public String editTransform( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    boolean refresh = false;
    String transformName = null;
    try {
      String name = transformMeta.getName();

      // Before we do anything, let's store the situation the way it
      // was...
      //
      TransformMeta before = (TransformMeta) transformMeta.clone();
      TransformDialogInterface dialog = getTransformDialog( transformMeta.getTransformMetaInterface(), pipelineMeta, name );
      if ( dialog != null ) {
        dialog.setMetaStore( hopUi.getMetaStore() );
        transformName = dialog.open();
      }

      if ( !Utils.isEmpty( transformName ) ) {
        // Force the recreation of the transform IO metadata object. (cached by default)
        //
        transformMeta.getTransformMetaInterface().resetTransformIoMeta();

        //
        // See if the new name the user enter, doesn't collide with
        // another transform.
        // If so, change the transformName and warn the user!
        //
        String newname = transformName;
        TransformMeta smeta = pipelineMeta.findTransform( newname, transformMeta );
        int nr = 2;
        while ( smeta != null ) {
          newname = transformName + " " + nr;
          smeta = pipelineMeta.findTransform( newname );
          nr++;
        }
        if ( nr > 2 ) {
          transformName = newname;
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.TransformnameExists.Message", transformName ) );
          mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.TransformnameExists.Title" ) );
          mb.open();
        }

        if ( !transformName.equals( name ) ) {
          refresh = true;
        }

        TransformMeta newTransformMeta = (TransformMeta) transformMeta.clone();
        newTransformMeta.setName( transformName );
        pipelineMeta.clearCaches();
        pipelineMeta.notifyAllListeners( transformMeta, newTransformMeta );
        transformMeta.setName( transformName );

        //
        // OK, so the transform has changed...
        // Backup the situation for undo/redo
        //
        TransformMeta after = (TransformMeta) transformMeta.clone();
        /**
         * TODO: Create new Undo/Redo system
         hopUi.addUndoChange( pipelineMeta, new TransformMeta[] { before }, new TransformMeta[] { after }, new int[] { pipelineMeta
         .indexOfTransform( transformMeta ) } );
         */
      }
      pipelineGraph.redraw(); // name is displayed on the graph too.

      // TODO: verify "double pathway" transforms for bug #4365
      // After the transform was edited we can complain about the possible
      // deadlock here.
      //
    } catch ( Throwable e ) {
      if ( hopUi.getShell().isDisposed() ) {
        return null;
      }
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.UnableOpenDialog.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.UnableOpenDialog.Message" ), e );
    }

    return transformName;
  }


  /**
   * Allocate new transform, optionally open and rename it.
   *
   * @param id          Id of the new transform
   * @param name        Name of the new transform
   * @param description Description of the type of transform
   * @param openit      Open the dialog for this transform?
   * @param rename      Rename this transform?
   * @return The newly created TransformMeta object.
   */
  public TransformMeta newTransform( PipelineMeta pipelineMeta, String id, String name, String description, boolean openit, boolean rename, Point location ) {
    try {
      TransformMeta transformMeta = null;

      // See if we need to rename the transform to avoid doubles!
      if ( rename && pipelineMeta.findTransform( name ) != null ) {
        int i = 2;
        String newName = name + " " + i;
        while ( pipelineMeta.findTransform( newName ) != null ) {
          i++;
          newName = name + " " + i;
        }
        name = newName;
      }

      PluginRegistry registry = PluginRegistry.getInstance();
      PluginInterface transformPlugin = id != null ? registry.findPluginWithId( TransformPluginType.class, id )
        : registry.findPluginWithName( TransformPluginType.class, description );

      try {
        if ( transformPlugin != null ) {
          TransformMetaInterface info = (TransformMetaInterface) registry.loadClass( transformPlugin );

          info.setDefault();

          if ( openit ) {
            TransformDialogInterface dialog = this.getTransformDialog( info, pipelineMeta, name );
            if ( dialog != null ) {
              name = dialog.open();
            }
          }
          transformMeta = new TransformMeta( transformPlugin.getIds()[ 0 ], name, info );

          if ( name != null ) {
            // OK pressed in the dialog: we have a transform-name
            String newName = name;
            TransformMeta candiateTransformMeta = pipelineMeta.findTransform( newName );
            int nr = 2;
            while ( candiateTransformMeta != null ) {
              newName = name + " " + nr;
              candiateTransformMeta = pipelineMeta.findTransform( newName );
              nr++;
            }
            if ( nr > 2 ) {
              transformMeta.setName( newName );
              MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
              // "This transformName already exists.  HopGui changed the transformName to ["+newName+"]"
              mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.ChangeTransformname.Message", newName ) );
              mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.ChangeTransformname.Title" ) );
              mb.open();
            }
            transformMeta.setLocation( location.x, location.y ); // default location at (20,20)
            pipelineMeta.addTransform( transformMeta );
            hopUi.undoDelegate.addUndoNew( pipelineMeta, new TransformMeta[] { transformMeta }, new int[] { pipelineMeta.indexOfTransform( transformMeta ) } );

            // Also store it in the pluginHistory list...
            hopUi.getProps().increasePluginHistory( transformPlugin.getIds()[ 0 ] );

          } else {
            return null; // Cancel pressed in dialog.
          }
          pipelineGraph.updateGui();
        }
      } catch ( HopException e ) {
        String filename = transformPlugin.getErrorHelpFile();
        if ( !Utils.isEmpty( filename ) ) {
          // OK, in stead of a normal error message, we give back the
          // content of the error help file... (HTML)
          FileInputStream fis = null;
          try {
            StringBuilder content = new StringBuilder();

            fis = new FileInputStream( new File( filename ) );
            int ch = fis.read();
            while ( ch >= 0 ) {
              content.append( (char) ch );
              ch = fis.read();
            }

            ShowBrowserDialog sbd = new ShowBrowserDialog( hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ErrorHelpText.Title" ), content.toString() );
            sbd.open();
          } catch ( Exception ex ) {
            new ErrorDialog( hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ErrorShowingHelpText.Title" ),
              BaseMessages.getString( PKG, "HopGui.Dialog.ErrorShowingHelpText.Message" ), ex );
          } finally {
            if ( fis != null ) {
              try {
                fis.close();
              } catch ( Exception ex ) {
                hopUi.getLog().logError( "Error closing plugin help file", ex );
              }
            }
          }
        } else {
          new ErrorDialog( hopUi.getShell(),
            // "Error creating transform"
            // "I was unable to create a new transform"
            BaseMessages.getString( PKG, "HopGui.Dialog.UnableCreateNewTransform.Title" ), BaseMessages.getString(
            PKG, "HopGui.Dialog.UnableCreateNewTransform.Message" ), e );
        }
        return null;
      } catch ( Throwable e ) {
        if ( !hopUi.getShell().isDisposed() ) {
          new ErrorDialog( hopUi.getShell(),
            // "Error creating transform"
            BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingTransform.Title" ), BaseMessages.getString(
            PKG, "HopGui.Dialog.UnableCreateNewTransform.Message" ), e );
        }
        return null;
      }

      return transformMeta;
    } finally {
      pipelineGraph.redraw();
    }
  }

  public void editTransformPartitioning( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    String[] schemaNames;
    try {
      schemaNames = hopUi.partitionManager.getNamesArray();
    } catch ( HopException e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.Title" ),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorFetchingFromRepo.PartitioningSchemas" ),
        e
      );
      return;
    }
    try {
      /*Check if Partition schema has already defined*/
      if ( isDefinedSchemaExist( schemaNames ) ) {

        /*Prepare settings for Method selection*/
        PluginRegistry registry = PluginRegistry.getInstance();
        List<PluginInterface> plugins = registry.getPlugins( PartitionerPluginType.class );
        int exactSize = TransformPartitioningMeta.methodDescriptions.length + plugins.size();
        PartitionSettings partitionSettings = new PartitionSettings( exactSize, pipelineMeta, transformMeta, hopUi.partitionManager );
        partitionSettings.fillOptionsAndCodesByPlugins( plugins );

        /*Method selection*/
        PartitionMethodSelector methodSelector = new PartitionMethodSelector();
        String partitionMethodDescription = methodSelector.askForPartitionMethod( hopUi.getShell(), partitionSettings );
        if ( !StringUtil.isEmpty( partitionMethodDescription ) ) {
          String method = partitionSettings.getMethodByMethodDescription( partitionMethodDescription );
          int methodType = TransformPartitioningMeta.getMethodType( method );

          partitionSettings.updateMethodType( methodType );
          partitionSettings.updateMethod( method );

          /*Schema selection*/
          MethodProcessor methodProcessor = MethodProcessorFactory.create( methodType );
          methodProcessor.schemaSelection( partitionSettings, hopUi.getShell(), new IPartitionSchemaSelection() {
            @Override public String schemaFieldSelection( Shell shell, PartitionSettings settings ) throws HopException {
              TransformPartitioningMeta partitioningMeta = settings.getTransformMeta().getTransformPartitioningMeta();
              TransformDialogInterface dialog = getPartitionerDialog( shell, settings.getTransformMeta(), partitioningMeta, settings.getPipelineMeta() );
              return dialog.open();
            }
          } );
        }
        transformMeta.setChanged();
        hopUi.undoDelegate.addUndoChange( partitionSettings.getPipelineMeta(), new TransformMeta[] { partitionSettings.getBefore() },
         new TransformMeta[] { partitionSettings.getAfter() }, new int[] { partitionSettings.getPipelineMeta()
         .indexOfTransform( partitionSettings.getTransformMeta() ) }
         );
        pipelineGraph.redraw();
      }
    } catch ( Exception e ) {
      new ErrorDialog(
        hopUi.getShell(), "Error",
        "There was an unexpected error while editing the partitioning method specifics:", e );
    }
  }

  public boolean isDefinedSchemaExist( String[] schemaNames ) {
    // Before we start, check if there are any partition schemas defined...
    if ( ( schemaNames == null ) || ( schemaNames.length == 0 ) ) {
      MessageBox box = new MessageBox( hopUi.getShell(), SWT.ICON_ERROR | SWT.OK );
      box.setText( "Create a partition schema" );
      box.setMessage( "You first need to create one or more partition schemas before you can select one!" );
      box.open();
      return false;
    }
    return true;
  }

  public TransformDialogInterface getPartitionerDialog( Shell shell, TransformMeta transformMeta, TransformPartitioningMeta partitioningMeta,
                                                        PipelineMeta pipelineMeta ) throws HopException {
    Partitioner partitioner = partitioningMeta.getPartitioner();
    String dialogClassName = partitioner.getDialogClassName();

    Class<?> dialogClass;
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, TransformMeta.class, TransformPartitioningMeta.class, PipelineMeta.class };
    Object[] paramArgs = new Object[] { shell, transformMeta, partitioningMeta, pipelineMeta };
    Constructor<?> dialogConstructor;
    try {
      dialogClass = partitioner.getClass().getClassLoader().loadClass( dialogClassName );
      dialogConstructor = dialogClass.getConstructor( paramClasses );
      return (TransformDialogInterface) dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      throw new HopException( "Unable to open dialog of partitioning method", e );
    }
  }

  public void editTransformErrorHandling( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    if ( transformMeta != null && transformMeta.supportsErrorHandling() ) {
      TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
      if ( transformErrorMeta == null ) {
        transformErrorMeta = new TransformErrorMeta( pipelineMeta, transformMeta );
      }
      List<TransformMeta> targetTransforms = pipelineMeta.findNextTransforms( transformMeta );

      // now edit this transformErrorMeta object:
      TransformErrorMetaDialog dialog =
        new TransformErrorMetaDialog( hopUi.getShell(), transformErrorMeta, pipelineMeta, targetTransforms );
      if ( dialog.open() ) {
        transformMeta.setTransformErrorMeta( transformErrorMeta );
        transformMeta.setChanged();
        pipelineGraph.redraw();
      }
    }
  }

  public void delTransforms( PipelineMeta pipelineMeta, List<TransformMeta> transforms ) {
    try {
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.PipelineBeforeDeleteTransforms.id, transforms );
    } catch ( HopException e ) {
      return;
    }

    // Hops belonging to the deleting transforms are placed in a single transaction and removed.
    List<PipelineHopMeta> pipelineHops = new ArrayList<>();
    int[] hopIndexes = new int[ pipelineMeta.nrPipelineHops() ];
    int hopIndex = 0;
    for ( int i = pipelineMeta.nrPipelineHops() - 1; i >= 0; i-- ) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop( i );
      for ( int j = 0; j < transforms.size() && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromTransform().equals( transforms.get( j ) ) || hi.getToTransform().equals( transforms.get( j ) ) ) {
          int idx = pipelineMeta.indexOfPipelineHop( hi );
          pipelineHops.add( (PipelineHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          pipelineMeta.removePipelineHop( idx );
          hopIndex++;
          break;
        }
      }
    }
    if ( !pipelineHops.isEmpty() ) {
      PipelineHopMeta[] hops = pipelineHops.toArray( new PipelineHopMeta[ pipelineHops.size() ] );
      hopUi.undoDelegate.addUndoDelete( pipelineMeta, hops, hopIndexes );
    }

    // Deleting transforms are placed all in a single transaction and removed.
    int[] positions = new int[ transforms.size() ];
    for ( int i = 0; i < transforms.size(); i++ ) {
      int pos = pipelineMeta.indexOfTransform( transforms.get( i ) );
      pipelineMeta.removeTransform( pos );
      positions[ i ] = pos;
    }
    hopUi.undoDelegate.addUndoDelete( pipelineMeta, transforms.toArray( new TransformMeta[ 0 ] ), positions );

    pipelineGraph.redraw();
  }

  public void delTransform( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    delTransforms( pipelineMeta, Arrays.asList( transformMeta ) );
  }
}
