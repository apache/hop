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

package org.apache.hop.neo4j.shared;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.eclipse.swt.widgets.Composite;

/**
 * Selects a Neo4j connection or a graph database connection of a database which speaks Cypher,
 * whatever the protocol. For use in {@code @GuiWidgetElement(metadataSelectionLine = ...)}.
 */
public class CypherConnectionSelectionLine extends NeoConnectionSelectionLine {

  public CypherConnectionSelectionLine(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Composite parentComposite,
      int flags,
      String labelText,
      String toolTipText) {
    super(variables, metadataProvider, parentComposite, flags, labelText, toolTipText, true);
  }

  @Override
  protected List<String> getConnectionNames() throws HopException {
    return NeoConnectionUtils.getCypherConnectionNames(getMetadataProvider());
  }
}
