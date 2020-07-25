package org.apache.hop.ui.hopgui;

import org.eclipse.rap.rwt.SingletonUtil;
import org.eclipse.rap.rwt.service.ServerPushSession;

public class ServerPushSessionFacadeImpl extends ServerPushSessionFacade {

  @Override
  void startInternal() {
    SingletonUtil.getSessionInstance( ServerPushSession.class ).start();
  }

  @Override
  void stopInternal() {
    SingletonUtil.getSessionInstance( ServerPushSession.class ).stop();
  }
}
