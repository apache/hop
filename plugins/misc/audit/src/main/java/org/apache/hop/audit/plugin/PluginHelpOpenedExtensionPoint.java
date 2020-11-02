/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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
package org.apache.hop.audit.plugin;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPlugin;

@ExtensionPoint(
    id = "AuditPluginHelpOpened",
    description =
        "Audit extension which catches when plugin help opened and sends to audit manager.",
    extensionPointId = "AuditPluginHelpOpened")
/**
 * This extension point for capturing the user action of looking for help. By having this data a
 * plugin developer can take decision on improving help content.
 */
public class PluginHelpOpenedExtensionPoint implements IExtensionPoint<IPlugin> {

  /**
   * Work In Progress: As of now it just print it in the log
   *
   * <p>Design Thoughts: It will extract the details from plugin object and send it to a audit
   * manager which aware where to send the audit information (log, hop server, analytics server).
   */
  @Override
  public void callExtensionPoint(ILogChannel log, IPlugin plugin) throws HopException {

    log.logBasic("Plugin help opened -" + plugin.getName());
  }
}
