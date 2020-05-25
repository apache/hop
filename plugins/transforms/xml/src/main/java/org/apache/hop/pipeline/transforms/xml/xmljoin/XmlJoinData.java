/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.xml.xmljoin;


import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * @author Ingo Klose
 * @since 30-apr-2008
 */
public class XmlJoinData extends BaseTransformData implements ITransformData {
  public IRowMeta outputRowMeta;

  public IRowSet TargetRowSet;
  public IRowSet SourceRowSet;

  public Object[] outputRowData;

  public Document targetDOM;

  public Node targetNode;

  public NodeList targetNodes;

  public String XPathStatement;

  public int iSourceXMLField = -1;
  public int iCompareFieldID = -1;

  /**
     *
     */
  public XmlJoinData() {
    super();

  }

}
