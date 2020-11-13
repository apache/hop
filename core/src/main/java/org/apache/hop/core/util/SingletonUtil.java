package org.apache.hop.core.util;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

import java.lang.reflect.Method;
import java.util.List;

public class SingletonUtil {
  public static final List<String> getValuesList( String guiPluginId, String singletonClassName, String methodName) throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin guiPlugin = registry.getPlugin( GuiPluginType.class, guiPluginId );
      ClassLoader classLoader = registry.getClassLoader( guiPlugin );

      Class<?> singletonClass = classLoader.loadClass( singletonClassName );
      Method getInstanceMethod = singletonClass.getDeclaredMethod("getInstance", new Class[] { });
      Object singleton = getInstanceMethod.invoke( null, new Object[] {} );

      Method method = singletonClass.getMethod( methodName );
      List<String> values = (List<String>) method.invoke( singleton, new Object[] {} );

      return values;
    } catch ( Exception e ) {
      throw new HopException("Unable to get list of values from class "+singletonClassName+" with method "+methodName, e);
    }
  }
}
