package org.apache.hop.ui.hopgui;

import java.text.MessageFormat;

public class ImplementationLoader {
  public static Object newInstance(final Class type) {
    String name = type.getName();
    Object result = null;
    try {
      // TODO: https://stackoverflow.com/questions/234600/can-i-use-class-newinstance-with-constructor-arguments
      // TODO: https://stackoverflow.com/questions/195321/why-is-class-newinstance-evil
      result = type.getClassLoader().loadClass( name + "Impl" ).newInstance();
    } catch (Throwable throwable) {
      String txt = "Could not load implementation for {0}";
      String msg = MessageFormat.format(txt, new Object[] {name});
      throw new RuntimeException(msg, throwable);
    }
    return result;
  }
}
