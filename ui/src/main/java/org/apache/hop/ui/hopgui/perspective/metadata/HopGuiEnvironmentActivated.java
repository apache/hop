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

package org.apache.hop.ui.hopgui.perspective.metadata;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Reload the available metadata when an environment is changed...
 */
@ExtensionPoint(id = "HopGuiEnvironmentActivated", extensionPointId = "EnvironmentActivated", description = "Reload the available metadata when an environment is changed")
public class HopGuiEnvironmentActivated implements IExtensionPoint<String> {

	@Override public void callExtensionPoint( ILogChannel log, String environmentName ) throws HopException {
		
		MetadataPerspective perspective = (MetadataPerspective) HopGui.getInstance().getPerspectiveManager().findPerspective( MetadataPerspective.class );
			  
		System.out.println("Switch environnement");
		if ( perspective!=null ) { 
			perspective.refresh();
		}
	}
}
