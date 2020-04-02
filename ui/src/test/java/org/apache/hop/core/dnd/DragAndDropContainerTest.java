/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.dnd;

import org.apache.hop.core.exception.HopException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DragAndDropContainerTest {

  @Test
  public void newDNDContainer() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name" );

    assertNotNull( dnd );
    assertNull( dnd.getId() );
    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, dnd.getType() );
    assertEquals( "BaseTransform", dnd.getTypeCode() );
    assertEquals( "Transform Name", dnd.getData() );
  }

  @Test
  public void newDNDContainerWithId() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name", "TransformID" );

    assertNotNull( dnd );
    assertEquals( "TransformID", dnd.getId() );
    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, dnd.getType() );
    assertEquals( "BaseTransform", dnd.getTypeCode() );
    assertEquals( "Transform Name", dnd.getData() );
  }

  @Test
  public void setId() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name" );
    dnd.setId( "TransformID" );

    assertEquals( "TransformID", dnd.getId() );
  }

  @Test
  public void setData() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name" );
    dnd.setData( "Another Transform" );

    assertEquals( "Another Transform", dnd.getData() );
  }

  @Test
  public void setType() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_JOB_ENTRY, "Transform Name" );
    dnd.setType( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE );

    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, dnd.getType() );
  }

  @Test
  public void getTypeFromCode() {
    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, DragAndDropContainer.getType( "BaseTransform" ) );
  }

  @Test
  public void getXML() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name" );

    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DragAndDrop>\n"
      + "  <DragType>BaseTransform</DragType>\n"
      + "  <Data>VHJhbnNmb3JtIE5hbWU=</Data>\n"
      + "</DragAndDrop>\n";

    assertEquals( xml, dnd.getXml() );
  }

  @Test
  public void getXMLWithId() {
    DragAndDropContainer dnd = new DragAndDropContainer( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, "Transform Name", "TransformID" );

    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DragAndDrop>\n"
      + "  <ID>TransformID</ID>\n"
      + "  <DragType>BaseTransform</DragType>\n"
      + "  <Data>VHJhbnNmb3JtIE5hbWU=</Data>\n"
      + "</DragAndDrop>\n";

    assertEquals( xml, dnd.getXml() );
  }

  @Test
  public void newFromXML() throws HopException {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DragAndDrop>\n"
      + "  <DragType>BaseTransform</DragType>\n"
      + "  <Data>VHJhbnNmb3JtIE5hbWU=</Data>\n"
      + "</DragAndDrop>\n";

    DragAndDropContainer dnd = new DragAndDropContainer( xml );

    assertNotNull( dnd );
    assertNull( dnd.getId() );
    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, dnd.getType() );
    assertEquals( "BaseTransform", dnd.getTypeCode() );
    assertEquals( "Transform Name", dnd.getData() );
  }

  @Test
  public void newFromXMLWithId() throws HopException {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<DragAndDrop>\n"
      + "  <ID>TransformID</ID>\n"
      + "  <DragType>BaseTransform</DragType>\n"
      + "  <Data>VHJhbnNmb3JtIE5hbWU=</Data>\n"
      + "</DragAndDrop>\n";

    DragAndDropContainer dnd = new DragAndDropContainer( xml );

    assertNotNull( dnd );
    assertEquals( "TransformID", dnd.getId() );
    assertEquals( DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE, dnd.getType() );
    assertEquals( "BaseTransform", dnd.getTypeCode() );
    assertEquals( "Transform Name", dnd.getData() );
  }
}
