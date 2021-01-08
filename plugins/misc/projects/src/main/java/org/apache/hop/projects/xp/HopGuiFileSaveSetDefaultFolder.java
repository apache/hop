/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;

@ExtensionPoint(
  id = "HopGuiFileSaveSetDefaultFolder",
  extensionPointId = "HopGuiFileSaveDialog",
  description = "When HopGui saves a file under a new name it presents a dialog. We want to set the default folder to the project home folder"
)
public class HopGuiFileSaveSetDefaultFolder extends HopGuiFileDefaultFolder implements IExtensionPoint<HopGuiFileDialogExtension> {

}
