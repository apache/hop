package org.apache.hop.ui.core.gui;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Listener;

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
      if (singleton==null) {
        LogChannel.UI.logError( "Could not get instance of class '" + toolbarItem.getListenerClass() +" for toolbar item "+toolbarItem+", combo values method : "+toolbarItem.getGetComboValuesMethod() );
        return new String[] {};
      }

      // TODO: create a method finder where we can simply give a list of objects that we have available
      // You can find them in any order that the developer chose and just pass them that way.
      //
      Method method;
      boolean withArguments = true;
      try {
        method = singleton.getClass().getMethod( toolbarItem.getGetComboValuesMethod(), ILogChannel.class, IHopMetadataProvider.class );
      } catch(NoSuchMethodException nsme) {
        // Try to find the method without arguments...
        //
        try {
          method = singleton.getClass().getMethod( toolbarItem.getGetComboValuesMethod() );
          withArguments = false;
        } catch(NoSuchMethodException nsme2) {
          throw new HopException( "Unable to find method '" + toolbarItem.getGetComboValuesMethod() + "' without parameters or with parameters ILogChannel and IHopMetadataProvider in class '" + toolbarItem.getListenerClass() + "'", nsme2 );
        }
      }
      List<String> values;
      if (withArguments) {
        values =  (List<String>) method.invoke( singleton, LogChannel.UI, HopGui.getInstance().getMetadataProvider() );
      } else {
        values =  (List<String>) method.invoke( singleton  );
      }
      return values.toArray( new String[ 0 ] );
    } catch ( Exception e ) {
      LogChannel.UI.logError( "Error getting list of combo items for method '" + toolbarItem.getGetComboValuesMethod() + "' in class : " + toolbarItem.getListenerClass(), e );
      return new String[] {};
    }
  }

  protected Listener getListener( ClassLoader classLoader, String listenerClassName, String listenerMethodName ) {

    // Call the method to which the GuiToolbarElement annotation belongs.
    //
    return e -> {
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
    };
  }
}
