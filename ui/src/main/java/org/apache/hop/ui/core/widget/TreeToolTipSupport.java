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

package org.apache.hop.ui.core.widget;

import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.jface.window.DefaultToolTip;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

public class TreeToolTipSupport extends DefaultToolTip {

	public TreeToolTipSupport(Control control) {
		super(control);	
	}

	@Override
	protected final String getText(Event event) {
		Tree tree = (Tree) event.widget;
		TreeItem item = tree.getItem(new Point(event.x, event.y));
		if ( item!=null) {
			return (String) item.getData(MetadataPerspective.KEY_HELP);
		}
		return null;
	}

	@Override
	protected boolean shouldCreateToolTip(Event event) {
		return super.shouldCreateToolTip(event) && (getText(event) != null);
	}
}
