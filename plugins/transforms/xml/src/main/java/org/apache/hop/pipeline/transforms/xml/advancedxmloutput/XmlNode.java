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

import com.google.common.base.Enums;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One node in the transform's hierarchical XML tree: element, attribute, or document fragment.
 * Exactly one element must be the row loop; ancestors of the loop may be group-by nodes.
 */
@Getter
@Setter
public class XmlNode {

  @SuppressWarnings("java:S115")
  public enum NodeKind {
    /** Standard XML element with optional text content and/or children. */
    Element,
    /** XML attribute on the parent element. */
    Attribute,
    /** Pre-built XML fragment inserted as parsed nodes (requires the source field to hold XML). */
    DocumentFragment;

    public static NodeKind getIfPresent(String name) {
      return Enums.getIfPresent(NodeKind.class, name).or(Element);
    }
  }

  /** Local name of the element or attribute. */
  @HopMetadataProperty(key = "name")
  private String name;

  /** Optional XML namespace URI for this element (ignored for attributes in v1). */
  @HopMetadataProperty(key = "namespace")
  private String namespace;

  /** Element / Attribute / DocumentFragment. */
  @HopMetadataProperty(key = "kind")
  private NodeKind kind;

  /**
   * Name of the input field whose value provides this node's content (for an element) or value (for
   * an attribute / document fragment). Empty / null means "static node".
   */
  @HopMetadataProperty(key = "mapped_field")
  private String mappedField;

  /** Static text used when {@link #mappedField} is empty (or the field value is null). */
  @HopMetadataProperty(key = "default_value")
  private String defaultValue;

  /** Optional Hop value-type override (one of {@link ValueMetaBase} type codes). 0 = auto. */
  @HopMetadataProperty(key = "type", intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int type;

  /** Optional conversion mask. */
  @HopMetadataProperty(key = "format")
  private String format;

  @HopMetadataProperty(key = "length")
  private int length;

  @HopMetadataProperty(key = "precision")
  private int precision;

  @HopMetadataProperty(key = "currency")
  private String currencySymbol;

  @HopMetadataProperty(key = "decimal")
  private String decimalSymbol;

  @HopMetadataProperty(key = "group")
  private String groupingSymbol;

  /** Output this node even when its value is null. */
  @HopMetadataProperty(key = "force_create")
  private boolean forceCreate;

  /** Marks this element as the row-loop element (exactly one in the tree). */
  @HopMetadataProperty(key = "loop")
  private boolean loop;

  /**
   * Marks this element as a group-by ancestor of the loop element. Consecutive input rows that
   * share the same value of {@link #mappedField} are emitted under a single occurrence of this
   * element.
   */
  @HopMetadataProperty(key = "group_by")
  private boolean groupBy;

  /**
   * For {@link NodeKind#DocumentFragment}: skip the outer element in the field value when it
   * duplicates the parent element in the tree.
   */
  @HopMetadataProperty(key = "strip_outer_fragment_element")
  private boolean stripOuterFragmentElement;

  /** Children (only meaningful for {@link NodeKind#Element}). */
  @HopMetadataProperty(key = "node", groupKey = "children", isExcludedFromInjection = true)
  private List<XmlNode> children;

  public XmlNode() {
    this.kind = NodeKind.Element;
    this.length = -1;
    this.precision = -1;
    this.type = 0;
    this.children = new ArrayList<>();
  }

  public XmlNode(String name, NodeKind kind) {
    this();
    this.name = name;
    this.kind = kind;
  }

  public XmlNode(XmlNode other) {
    this();
    this.name = other.name;
    this.namespace = other.namespace;
    this.kind = other.kind;
    this.mappedField = other.mappedField;
    this.defaultValue = other.defaultValue;
    this.type = other.type;
    this.format = other.format;
    this.length = other.length;
    this.precision = other.precision;
    this.currencySymbol = other.currencySymbol;
    this.decimalSymbol = other.decimalSymbol;
    this.groupingSymbol = other.groupingSymbol;
    this.forceCreate = other.forceCreate;
    this.loop = other.loop;
    this.groupBy = other.groupBy;
    this.stripOuterFragmentElement = other.stripOuterFragmentElement;
    if (other.children != null) {
      for (XmlNode c : other.children) {
        this.children.add(new XmlNode(c));
      }
    }
  }

  /** Convenience: returns true if this node has any direct children of element kind. */
  public boolean hasElementChildren() {
    if (children == null) {
      return false;
    }
    for (XmlNode c : children) {
      if (c.kind == NodeKind.Element || c.kind == NodeKind.DocumentFragment) {
        return true;
      }
    }
    return false;
  }

  /** Convenience: returns true if this node has any direct children of attribute kind. */
  public boolean hasAttributeChildren() {
    if (children == null) {
      return false;
    }
    for (XmlNode c : children) {
      if (c.kind == NodeKind.Attribute) {
        return true;
      }
    }
    return false;
  }

  public void addChild(XmlNode child) {
    if (children == null) {
      children = new ArrayList<>();
    }
    children.add(child);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof XmlNode that)) {
      return false;
    }
    return loop == that.loop
        && groupBy == that.groupBy
        && stripOuterFragmentElement == that.stripOuterFragmentElement
        && forceCreate == that.forceCreate
        && type == that.type
        && length == that.length
        && precision == that.precision
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && kind == that.kind
        && Objects.equals(mappedField, that.mappedField)
        && Objects.equals(defaultValue, that.defaultValue)
        && Objects.equals(format, that.format)
        && Objects.equals(currencySymbol, that.currencySymbol)
        && Objects.equals(decimalSymbol, that.decimalSymbol)
        && Objects.equals(groupingSymbol, that.groupingSymbol)
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kind, loop, groupBy, mappedField);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(kind == NodeKind.Attribute ? "@" : "<").append(name == null ? "" : name);
    if (loop) {
      sb.append("[loop]");
    }
    if (groupBy) {
      sb.append("[group]");
    }
    if (mappedField != null && !mappedField.isEmpty()) {
      sb.append("=").append(mappedField);
    }
    return sb.toString();
  }
}
