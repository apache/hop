package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

@Transform(
    id = "ArrowDecode",
    name = "Arrow Decode",
    description = "Decodes Arrow data types into Hop fields",
    image = "arrow_decode.svg",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/arrow-decode.html",
    keywords = "i18n::ArrowDecodeMeta.keyword")
public class ArrowDecodeMeta extends BaseTransformMeta<ArrowDecode, ArrowDecodeData> {
  public static final Class<?> PKG = ArrowDecodeMeta.class;

  @HopMetadataProperty(key = "source_field")
  private String sourceFieldName = "arrow";

  @HopMetadataProperty(key = "remove_source_field")
  private boolean removingSourceField = true;

  @HopMetadataProperty(key = "ignore_missing")
  private boolean ignoringMissingPaths = true;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<TargetField> targetFields = List.of();

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String transformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (TargetField targetField : targetFields) {
      try {
        IValueMeta valueMeta = targetField.createTargetValueMeta(variables);
        rowMeta.addValueMeta(valueMeta);
      } catch (HopException e) {
        throw new HopTransformException(
            "Error creating target field with name " + targetField.getTargetFieldName(), e);
      }
    }
  }

  /**
   * Gets sourceFieldName
   *
   * @return value of sourceFieldName
   */
  public String getSourceFieldName() {
    return sourceFieldName;
  }

  /** @param sourceFieldName The sourceFieldName to set */
  public void setSourceFieldName(String sourceFieldName) {
    this.sourceFieldName = sourceFieldName;
  }

  public boolean isRemovingSourceField() {
    return removingSourceField;
  }

  public void setRemovingSourceField(boolean removingSourceField) {
    this.removingSourceField = removingSourceField;
  }

  /**
   * Gets ignoringMissingPaths
   *
   * @return value of ignoringMissingPaths
   */
  public boolean isIgnoringMissingPaths() {
    return ignoringMissingPaths;
  }

  /** @param ignoringMissingPaths The ignoringMissingPaths to set */
  public void setIgnoringMissingPaths(boolean ignoringMissingPaths) {
    this.ignoringMissingPaths = ignoringMissingPaths;
  }

  /**
   * Gets targetFields
   *
   * @return value of targetFields
   */
  public List<TargetField> getTargetFields() {
    return targetFields;
  }

  /** @param targetFields The targetFields to set */
  public void setTargetFields(List<TargetField> targetFields) {
    this.targetFields = targetFields;
  }
}
