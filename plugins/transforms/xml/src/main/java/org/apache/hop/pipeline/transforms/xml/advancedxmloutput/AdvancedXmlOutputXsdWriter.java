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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Generates a sibling XSD schema document from an {@link XmlNode} tree and the input row metadata.
 *
 * <p>The generated schema is intentionally lean (one global element, nested complex types) so it
 * matches the StAX-streamed output shape exactly. Type information is derived from the input
 * row-meta of every node that carries a {@code mappedField}; nodes without a mapping fall back to
 * {@code xs:string}.
 *
 * <p>Multiplicity is set to {@code maxOccurs="unbounded"} for the loop element and any group-by
 * ancestors. Document-fragment nodes are emitted as {@code <xs:any/>} placeholders.
 */
final class AdvancedXmlOutputXsdWriter {

  private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema";
  private static final String XSD_PREFIX = "xs";
  private static final XMLOutputFactory FACTORY = XMLOutputFactory.newInstance();

  private AdvancedXmlOutputXsdWriter() {}

  /**
   * Writes the XSD for {@code root} into the file located at {@code xsdFilename}.
   *
   * @param xsdFilename fully-resolved physical path of the .xsd file to write
   * @param variables variable resolver used to materialize the {@link FileObject}
   * @param encoding output character encoding (UTF-8 if blank)
   * @param root root of the XML tree being described
   * @param rowMeta input row metadata used to map mapped fields to XSD types (may be {@code null})
   */
  static void write(
      String xsdFilename, IVariables variables, String encoding, XmlNode root, IRowMeta rowMeta)
      throws Exception {
    if (root == null) {
      return;
    }
    FileObject fo = HopVfs.getFileObject(xsdFilename, variables);
    try (OutputStream os = HopVfs.getOutputStream(fo, false)) {
      String enc = Utils.isEmpty(encoding) ? "UTF-8" : encoding;
      XMLStreamWriter w = FACTORY.createXMLStreamWriter(os, enc);
      w.writeStartDocument(enc, "1.0");
      w.writeCharacters("\n");

      String tns = root.getNamespace();
      w.writeStartElement(XSD_PREFIX, "schema", XSD_NS);
      w.writeNamespace(XSD_PREFIX, XSD_NS);
      if (!Utils.isEmpty(tns)) {
        w.writeAttribute("targetNamespace", tns);
        w.writeAttribute("elementFormDefault", "qualified");
        w.writeDefaultNamespace(tns);
      }

      writeElement(w, root, rowMeta, true);

      w.writeEndElement(); // schema
      w.writeEndDocument();
      w.close();
    }
  }

  private static void writeElement(
      XMLStreamWriter w, XmlNode node, IRowMeta rowMeta, boolean isRoot) throws Exception {

    boolean hasElementChildren = node.hasElementChildren();
    boolean hasAttributeChildren = node.hasAttributeChildren();
    boolean isLeaf = !hasElementChildren && !hasAttributeChildren;

    w.writeStartElement(XSD_PREFIX, "element", XSD_NS);
    w.writeAttribute("name", Utils.isEmpty(node.getName()) ? "_" : node.getName());

    if (!isRoot && (node.isLoop() || node.isGroupBy())) {
      w.writeAttribute("minOccurs", "0");
      w.writeAttribute("maxOccurs", "unbounded");
    }

    if (isLeaf) {
      w.writeAttribute("type", XSD_PREFIX + ":" + xsdSimpleTypeFor(node, rowMeta));
      w.writeEndElement();
      return;
    }

    w.writeStartElement(XSD_PREFIX, "complexType", XSD_NS);

    List<XmlNode> elements = childrenOfKind(node, XmlNode.NodeKind.Element);
    List<XmlNode> fragments = childrenOfKind(node, XmlNode.NodeKind.DocumentFragment);
    List<XmlNode> attributes = childrenOfKind(node, XmlNode.NodeKind.Attribute);

    boolean hasOwnText =
        !Utils.isEmpty(node.getMappedField()) || !Utils.isEmpty(node.getDefaultValue());
    if (hasOwnText && !node.isGroupBy()) {
      w.writeAttribute("mixed", "true");
    }

    if (!elements.isEmpty() || !fragments.isEmpty()) {
      w.writeStartElement(XSD_PREFIX, "sequence", XSD_NS);
      for (XmlNode c : elements) {
        writeElement(w, c, rowMeta, false);
      }
      for (XmlNode c : fragments) {
        w.writeStartElement(XSD_PREFIX, "any", XSD_NS);
        w.writeAttribute("processContents", "skip");
        if (c.isLoop() || c.isGroupBy()) {
          w.writeAttribute("minOccurs", "0");
          w.writeAttribute("maxOccurs", "unbounded");
        } else {
          w.writeAttribute("minOccurs", "0");
        }
        w.writeEndElement();
      }
      w.writeEndElement(); // sequence
    }

    for (XmlNode a : attributes) {
      w.writeStartElement(XSD_PREFIX, "attribute", XSD_NS);
      w.writeAttribute("name", Utils.isEmpty(a.getName()) ? "_" : a.getName());
      w.writeAttribute("type", XSD_PREFIX + ":" + xsdSimpleTypeFor(a, rowMeta));
      if (a.isForceCreate()) {
        w.writeAttribute("use", "required");
      } else {
        w.writeAttribute("use", "optional");
      }
      w.writeEndElement();
    }

    w.writeEndElement(); // complexType
    w.writeEndElement(); // element
  }

  private static List<XmlNode> childrenOfKind(XmlNode parent, XmlNode.NodeKind kind) {
    List<XmlNode> out = new ArrayList<>();
    if (parent.getChildren() != null) {
      for (XmlNode c : parent.getChildren()) {
        if (c.getKind() == kind) {
          out.add(c);
        }
      }
    }
    return out;
  }

  /** Maps a node's mapped field type to a built-in XSD simple type. Defaults to {@code string}. */
  static String xsdSimpleTypeFor(XmlNode node, IRowMeta rowMeta) {
    String field = node.getMappedField();
    if (Utils.isEmpty(field) || rowMeta == null) {
      return "string";
    }
    int idx = rowMeta.indexOfValue(field);
    if (idx < 0) {
      return "string";
    }
    IValueMeta vm = rowMeta.getValueMeta(idx);
    return switch (vm.getType()) {
      case IValueMeta.TYPE_INTEGER -> "long";
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER -> "decimal";
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> "dateTime";
      case IValueMeta.TYPE_BOOLEAN -> "boolean";
      case IValueMeta.TYPE_BINARY -> "base64Binary";
      default -> "string";
    };
  }
}
