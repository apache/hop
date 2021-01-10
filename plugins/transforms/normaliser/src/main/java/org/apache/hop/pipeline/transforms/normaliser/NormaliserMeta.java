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

package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * Created on 30-okt-2003
 *
 */

/*

DATE      PRODUCT1_NR  PRODUCT1_SL  PRODUCT2_NR PRODUCT2_SL PRODUCT3_NR PRODUCT3_SL
20030101            5          100           10         250           4         150

DATE      PRODUCT    Sales   Number
20030101  PRODUCT1     100        5
20030101  PRODUCT2     250       10
20030101  PRODUCT3     150        4

--> we need a mapping of fields with occurances.  (PRODUCT1_NR --> "PRODUCT1", PRODUCT1_SL --> "PRODUCT1", ...)
--> List of Fields with the type and the new fieldname to fill
--> PRODUCT1_NR, "PRODUCT1", Number
--> PRODUCT1_SL, "PRODUCT1", Sales
--> PRODUCT2_NR, "PRODUCT2", Number
--> PRODUCT2_SL, "PRODUCT2", Sales
--> PRODUCT3_NR, "PRODUCT3", Number
--> PRODUCT3_SL, "PRODUCT3", Sales

--> To parse this, we loop over the occurances of type: "PRODUCT1", "PRODUCT2" and "PRODUCT3"
--> For each of the occurance, we insert a record.

**/

@Transform(
    id = "Normaliser",
    name = "i18n::Normaliser.Name",
    description = "i18n::Normaliser.Description",
    image = "normaliser.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = {"transform"},
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/normaliser.html")
@InjectionSupported(
    localizationPrefix = "NormaliserMeta.Injection.",
    groups = {"FIELDS"})
public class NormaliserMeta extends BaseTransformMeta
    implements ITransformMeta<Normaliser, NormaliserData> {
  private static final Class<?> PKG = NormaliserMeta.class; // For Translator

  private String typeField; // Name of the new type-field.

  @InjectionDeep private NormaliserField[] normaliserFields = {};

  public NormaliserMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the typeField. */
  public String getTypeField() {
    return typeField;
  }

  /** @param typeField The typeField to set. */
  public void setTypeField(String typeField) {
    this.typeField = typeField;
  }

  public NormaliserField[] getNormaliserFields() {
    return normaliserFields;
  }

  public void setNormaliserFields(NormaliserField[] normaliserFields) {
    this.normaliserFields = normaliserFields;
  }

  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    String s;
    for (int i = 0; i < normaliserFields.length; i++) {
      s = normaliserFields[i].getName();
      if (s != null) {
        fieldNames.add(s.toLowerCase());
      }
    }
    return fieldNames;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    normaliserFields = new NormaliserField[nrFields];
    for (int i = 0; i < nrFields; i++) {
      normaliserFields[i] = new NormaliserField();
    }
  }

  @Override
  public Object clone() {
    NormaliserMeta retval = (NormaliserMeta) super.clone();

    int nrFields = normaliserFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.normaliserFields[i] = (NormaliserField) normaliserFields[i].clone();
    }

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      typeField = XmlHandler.getTagValue(transformNode, "typefield");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        normaliserFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        normaliserFields[i].setValue(XmlHandler.getTagValue(fnode, "value"));
        normaliserFields[i].setNorm(XmlHandler.getTagValue(fnode, "norm"));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "NormaliserMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    typeField = "typefield";

    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      normaliserFields[i].setName("field" + i);
      normaliserFields[i].setValue("value" + i);
      normaliserFields[i].setNorm("value" + i);
    }
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Get a unique list of the occurrences of the type
    //
    List<String> norm_occ = new ArrayList<>();
    List<String> field_occ = new ArrayList<>();
    int maxlen = 0;
    for (int i = 0; i < normaliserFields.length; i++) {
      if (!norm_occ.contains(normaliserFields[i].getNorm())) {
        norm_occ.add(normaliserFields[i].getNorm());
        field_occ.add(normaliserFields[i].getName());
      }

      if (normaliserFields[i].getValue().length() > maxlen) {
        maxlen = normaliserFields[i].getValue().length();
      }
    }

    // Then add the type field!
    //
    IValueMeta typefieldValue = new ValueMetaString(typeField);
    typefieldValue.setOrigin(name);
    typefieldValue.setLength(maxlen);
    row.addValueMeta(typefieldValue);

    // Loop over the distinct list of fieldNorm[i]
    // Add the new fields that need to be created.
    // Use the same data type as the original fieldname...
    //
    for (int i = 0; i < norm_occ.size(); i++) {
      String normname = norm_occ.get(i);
      String fieldname = field_occ.get(i);
      IValueMeta v = row.searchValueMeta(fieldname);
      if (v != null) {
        v = v.clone();
      } else {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "NormaliserMeta.Exception.UnableToFindField", fieldname));
      }
      v.setName(normname);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Now remove all the normalized fields...
    //
    for (int i = 0; i < normaliserFields.length; i++) {
      int idx = row.indexOfValue(normaliserFields[i].getName());
      if (idx >= 0) {
        row.removeValueMeta(idx);
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("   " + XmlHandler.addTagValue("typefield", typeField));

    retval.append("    <fields>");
    for (int i = 0; i < normaliserFields.length; i++) {
      retval.append("      <field>");
      retval.append("        " + XmlHandler.addTagValue("name", normaliserFields[i].getName()));
      retval.append("        " + XmlHandler.addTagValue("value", normaliserFields[i].getValue()));
      retval.append("        " + XmlHandler.addTagValue("norm", normaliserFields[i].getNorm()));
      retval.append("        </field>");
    }
    retval.append("      </fields>");

    return retval.toString();
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    String errorMessage = "";
    CheckResult cr;

    // Look up fields in the input stream <prev>
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NormaliserMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      boolean first = true;
      errorMessage = "";
      boolean errorFound = false;

      for (int i = 0; i < normaliserFields.length; i++) {
        String lufield = normaliserFields[i].getName();

        IValueMeta v = prev.searchValueMeta(lufield);
        if (v == null) {
          if (first) {
            first = false;
            errorMessage +=
                BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.FieldsNotFound") + Const.CR;
          }
          errorFound = true;
          errorMessage += "\t\t" + lufield + Const.CR;
        }
      }
      if (errorFound) {
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.AllFieldsFound"),
                transformMeta);
      }
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "NormaliserMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.TransformReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "NormaliserMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public Normaliser createTransform(
      TransformMeta transformMeta,
      NormaliserData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Normaliser(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public NormaliserData getTransformData() {
    return new NormaliserData();
  }

  public static class NormaliserField implements Cloneable {

    @Injection(name = "NAME", group = "FIELDS")
    private String name;

    @Injection(name = "VALUE", group = "FIELDS")
    private String value;

    @Injection(name = "NORMALISED", group = "FIELDS")
    private String norm;

    public NormaliserField() {}

    /** @return the name */
    public String getName() {
      return name;
    }

    /** @param name the name to set */
    public void setName(String name) {
      this.name = name;
    }

    /** @return the value */
    public String getValue() {
      return value;
    }

    /** @param value the value to set */
    public void setValue(String value) {
      this.value = value;
    }

    /** @return the norm */
    public String getNorm() {
      return norm;
    }

    /** @param norm the norm to set */
    public void setNorm(String norm) {
      this.norm = norm;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((norm == null) ? 0 : norm.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      NormaliserField other = (NormaliserField) obj;
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      if (norm == null) {
        if (other.norm != null) {
          return false;
        }
      } else if (!norm.equals(other.norm)) {
        return false;
      }
      if (value == null) {
        if (other.value != null) {
          return false;
        }
      } else if (!value.equals(other.value)) {
        return false;
      }
      return true;
    }

    @Override
    public Object clone() {
      try {
        NormaliserField retval = (NormaliserField) super.clone();
        return retval;
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
