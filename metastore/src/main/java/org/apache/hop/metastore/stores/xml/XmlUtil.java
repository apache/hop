/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.apache.hop.metastore.stores.xml;

import java.io.File;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class XmlUtil {

  public static final String META_FOLDER_NAME = "metastore";
  public static final String ELEMENT_TYPE_FILE_NAME = ".type.xml";

  public static String getNodeValue( Node node ) {
    if ( node == null ) {
      return null;
    }

    NodeList children = node.getChildNodes();
    for ( int i = 0; i < children.getLength(); i++ ) {
      Node child = children.item( i );
      if ( child.getNodeType() == Node.TEXT_NODE ) {
        return child.getNodeValue();
      }
    }
    return null;
  }

  public static String getNamespaceFolder( String rootFolder, String namespace ) {
    return rootFolder + File.separator + namespace;
  }

  public static String getElementTypeFolder( String rootFolder, String namespace, String elementTypeId ) {
    return getNamespaceFolder( rootFolder, namespace ) + File.separator + elementTypeId;
  }

  public static String getElementTypeFile( String rootFolder, String namespace, String elementTypeId ) {
    return getElementTypeFolder( rootFolder, namespace, elementTypeId ) + File.separator + ELEMENT_TYPE_FILE_NAME;
  }

  public static String getElementFile( String rootFolder, String namespace, String elementTypeId, String elementId ) {
    return getElementTypeFolder( rootFolder, namespace, elementTypeId ) + File.separator + elementId + ".xml";
  }

  public static DocumentBuilderFactory createSafeDocumentBuilderFactory() throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature( XMLConstants.FEATURE_SECURE_PROCESSING, true );
    factory.setFeature( "http://apache.org/xml/features/disallow-doctype-decl", true );
    return factory;
  }
}
