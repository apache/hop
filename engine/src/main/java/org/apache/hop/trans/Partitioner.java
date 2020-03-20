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

package org.apache.hop.trans;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.trans.step.StepPartitioningMeta;
import org.w3c.dom.Node;

/**
 * Defines methods needed for partitioner plugins. The main responsibilities are:
 * <ul>
 * <li><b>Maintain partitioner settings</b><br/>
 * The implementing class typically keeps track of partitioner settings using private fields with corresponding getters
 * and setters. The dialog class implementing StepDialogInterface is using the getters and setters to copy the user
 * supplied configuration in and out of the dialog.
 * <p>
 * The following interface method also falls into the area of maintaining settings:
 * <p>
 * <i><a href="#clone()">public Object clone()</a></i>
 * <p>
 * This method is called when a step containing partitioning configuration is duplicated in HopGui. It needs to return a
 * deep copy of this partitioner object. It is essential that the implementing class creates proper deep copies if the
 * configuration is stored in modifiable objects, such as lists or custom helper objects. The copy is created by first
 * calling super.clone(), and deep-copying any fields the partitioner may have declared.
 * <p>
 * <i><a href="#getInstance()">public Partitioner getInstance()</a></i>
 * <p>
 * This method is required to return a new instance of the partitioner class, with the plugin id and plugin description
 * inherited from the instance this function is called on.</li>
 * <li><b>Serialize partitioner settings</b><br/>
 * The plugin needs to be able to serialize its settings to XML. The interface methods are as
 * follows.
 * <p>
 * <i><a href="#getXML()">public String getXML()</a></i>
 * <p>
 * This method is called by PDI whenever the plugin needs to serialize its settings to XML. It is called when saving a
 * transformation in HopGui. The method returns an XML string, containing the serialized settings. The string contains a
 * series of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XMLHandler is typically
 * used to construct the XML string.
 * <p>
 * <i><a href="#loadXML(org.w3c.dom.Node)">public void loadXML(...)</a></i>
 * <p>
 * This method is called by PDI whenever a plugin needs to read its settings from XML. The XML node containing the
 * plugin's settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XMLHandler is
 * typically used to conveniently read the settings from the XML node.
 * <p>
 * <i><a href="#getDialogClassName()">public String getDialogClassName()</i></a> </li>
 * <li><b>Partition incoming rows during runtime</b><br/>
 * The class implementing Partitioner executes the actual logic that distributes the rows to available partitions.
 * <p>
 * This method is called with the row structure and the actual row as arguments. It must return the partition this row
 * will be sent to. The total number of partitions is available in the inherited field nrPartitions, and the return
 * value must be between 0 and nrPartitions-1.
 * <p>
 * <i><a href="">public int getPartition(...)</a></i></li>
 * </ul>
 */
public interface Partitioner {

  /**
   * Gets the single instance of Partitioner.
   *
   * @return single instance of Partitioner
   */
  public abstract Partitioner getInstance();

  /**
   * Gets the partition.
   *
   * @param rowMeta the row meta
   * @param r       the r
   * @return the partition
   * @throws HopException the kettle exception
   */
  public int getPartition( RowMetaInterface rowMeta, Object[] r ) throws HopException;

  /**
   * Sets the meta.
   *
   * @param meta the new meta
   */
  public void setMeta( StepPartitioningMeta meta );

  /**
   * Gets the id.
   *
   * @return the id
   */
  public String getId();

  /**
   * Gets the description.
   *
   * @return the description
   */
  public String getDescription();

  /**
   * Sets the id.
   *
   * @param id the new id
   */
  public void setId( String id );

  /**
   * Sets the description.
   *
   * @param description the new description
   */
  public void setDescription( String description );

  /**
   * Gets the dialog class name.
   *
   * @return the dialog class name
   */
  public String getDialogClassName();

  /**
   * Clone.
   *
   * @return the partitioner
   */
  public Partitioner clone();

  /**
   * Gets the xml.
   *
   * @return the xml
   */
  public String getXML();

  /**
   * Load xml.
   *
   * @param partitioningMethodNode the partitioning method node
   * @throws HopXMLException the kettle xml exception
   */
  public void loadXML( Node partitioningMethodNode ) throws HopXMLException;

}
