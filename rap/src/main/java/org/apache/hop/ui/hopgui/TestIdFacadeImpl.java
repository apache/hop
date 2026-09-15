/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.logging.LogChannel;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.service.JavaScriptExecutor;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.widgets.Widget;

/**
 * Hop Web: writes the id onto the widget's element as {@code data-hop-id}.
 *
 * <p>RAP gives each widget a generated id ({@code w123}) that it hands to the client, and the
 * client keeps a registry from that id to the widget object. Asking the client to set an HTML
 * attribute on the widget it already knows is therefore enough - no theming variant, no markup, and
 * nothing that changes what the widget is or how it behaves.
 *
 * <p>The attribute is remembered by the client widget and re-applied whenever it builds its
 * element, so it survives the widget being hidden, re-laid out or re-parented, and it does not
 * matter whether this runs before or after the widget first appears.
 *
 * <p>RAP's own {@code enableUITests} mode is not used: it renders the generated {@code w123} ids,
 * which depend on creation order and so name nothing durable.
 */
public class TestIdFacadeImpl extends TestIdFacade {

  /**
   * The script has to wait for the widget it names.
   *
   * <p>RAP appends a script where the code calls for it, but only creates widgets at the end of the
   * request, so a script that ran straight away would look up a widget the client has not been told
   * about yet. Yielding once is normally enough - the whole message is applied in one go - and the
   * retries cover a widget whose creation the server holds back to a later request.
   */
  private static final int MAX_TRIES = 20;

  private static final int RETRY_MILLIS = 50;

  @Override
  protected void setInternal(Widget widget, String testId) {
    try {
      String rwtId = WidgetUtil.getId(widget);
      if (rwtId == null || rwtId.isEmpty()) {
        return;
      }
      JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
      if (executor == null) {
        return;
      }
      executor.execute(
          "(function(){var tries=0;var name=function(){try{"
              + "var w=rwt.remote.ObjectRegistry.getObject('"
              + escape(rwtId)
              + "');"
              + "if(w&&w.setHtmlAttribute){w.setHtmlAttribute('"
              + TestIdFacade.ATTRIBUTE
              + "','"
              + escape(testId)
              + "');return;}"
              + "}catch(e){return;}"
              + "if(++tries<"
              + MAX_TRIES
              + "){setTimeout(name,"
              + RETRY_MILLIS
              + ");}};setTimeout(name,0);})();");
    } catch (Exception e) {
      // Naming a widget is never worth failing the GUI over.
      LogChannel.UI.logDebug("Could not set " + TestIdFacade.ATTRIBUTE + " " + testId + ": " + e);
    }
  }

  /**
   * Ids reach the browser inside a quoted JavaScript string, so anything that is not plainly an
   * identifier is replaced rather than escaped. Ids come from Hop's own annotations, but a plugin
   * is free to put anything in one.
   */
  private static String escape(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      boolean safe =
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '-'
              || c == '_'
              || c == '.'
              || c == ':';
      escaped.append(safe ? c : '-');
    }
    return escaped.toString();
  }
}
