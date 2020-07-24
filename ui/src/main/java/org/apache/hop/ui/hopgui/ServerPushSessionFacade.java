package org.apache.hop.ui.hopgui;

public abstract class ServerPushSessionFacade {
  private final static ServerPushSessionFacade IMPL;
  static {
    IMPL = (ServerPushSessionFacade) ImplementationLoader.newInstance( ServerPushSessionFacade.class );
  }

  public static void start() {
    IMPL.startInternal();
  }
  abstract void startInternal();

  public static void stop() {
    IMPL.stopInternal();
  }
  abstract void stopInternal();
}
