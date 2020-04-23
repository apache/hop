package org.apache.hop.ui.core.gui;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Widget;

import java.lang.reflect.Method;
import java.util.List;

public class BaseGuiWidgets {

  protected Object loadSingleTon( ClassLoader classLoader, String listenerClassName ) throws Exception {

    Class<?> listenerClass = classLoader.loadClass( listenerClassName );
    Method getInstanceMethod = listenerClass.getDeclaredMethod( "getInstance" );
    return getInstanceMethod.invoke( null, null );
  }

  protected String[] getComboItems( GuiToolbarItem toolbarItem ) {
    try {
      Object singleton = loadSingleTon( toolbarItem.getClassLoader(), toolbarItem.getListenerClass() );

      Method method = singleton.getClass().getMethod( toolbarItem.getGetComboValuesMethod(), ILogChannel.class, IMetaStore.class );
      if ( method == null ) {
        throw new HopException( "Unable to find method '" + toolbarItem.getGetComboValuesMethod() + "' with parameters ILogChannel and IMetaStore in class '" + toolbarItem.getListenerClass() + "'" );
      }
      List<String> values = (List<String>) method.invoke( singleton, LogChannel.UI, HopGui.getInstance().getMetaStore() );
      return values.toArray( new String[ 0 ] );
    } catch ( Exception e ) {
      LogChannel.UI.logError( "Error getting list of combo items for method '" + toolbarItem.getGetComboValuesMethod() + "' in class : " + toolbarItem.getListenerClass(), e );
      return new String[] {};
    }
  }

  protected void addSelectionListener( Widget item, ClassLoader classLoader, String listenerClassName, String listenerMethodName ) {

    // Call the method to which the GuiToolbarElement annotation belongs.
    //
    item.addListener( SWT.Selection, e -> {
      try {
        Object singleton = loadSingleTon( classLoader, listenerClassName );
        Method listenerMethod = singleton.getClass().getDeclaredMethod( listenerMethodName );
        if ( listenerMethod == null ) {
          throw new HopException( "Unable to find method " + listenerMethodName + " in class " + listenerClassName );
        }
        try {
          listenerMethod.invoke( singleton );
        } catch ( Exception ie ) {
          System.err.println( "Unable to call method " + listenerMethodName + " in class " + listenerClassName + " : " + ie.getMessage() );
          throw ie;
        }
      } catch ( Exception ex ) {
        ex.printStackTrace( System.err );
      }
    } );
  }
}
