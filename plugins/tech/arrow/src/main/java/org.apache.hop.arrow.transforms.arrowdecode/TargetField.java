package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class TargetField {
  @HopMetadataProperty(key = "source_field")
  private String sourceField;

  @HopMetadataProperty(key = "source_arrow_type")
  private String sourceAvroType;

  @HopMetadataProperty(key = "target_field_name")
  private String targetFieldName;

  @HopMetadataProperty(key = "target_type")
  private String targetType;

  public IValueMeta createTargetValueMeta(IVariables variables) throws HopException {
    String name = variables.resolve(Const.NVL(targetFieldName, sourceField));
    int type = ValueMetaFactory.getIdForValueMeta(variables.resolve(targetType));
    return ValueMetaFactory.createValueMeta(name, type);
  }

  /**
   * Gets sourceField
   *
   * @return value of sourceField
   */
  public String getSourceField() {
    return sourceField;
  }

  /** @param sourceField The sourcePath to set */
  public void setSourceField(String sourceField) {
    this.sourceField = sourceField;
  }

  /**
   * Gets sourceAvroType
   *
   * @return value of sourceAvroType
   */
  public String getSourceAvroType() {
    return sourceAvroType;
  }

  /** @param sourceAvroType The sourceAvroType to set */
  public void setSourceAvroType(String sourceAvroType) {
    this.sourceAvroType = sourceAvroType;
  }

  /**
   * Gets targetFieldName
   *
   * @return value of targetFieldName
   */
  public String getTargetFieldName() {
    return targetFieldName;
  }

  /** @param targetFieldName The targetFieldName to set */
  public void setTargetFieldName(String targetFieldName) {
    this.targetFieldName = targetFieldName;
  }

  /**
   * Gets targetType
   *
   * @return value of targetType
   */
  public String getTargetType() {
    return targetType;
  }

  /** @param targetType The targetType to set */
  public void setTargetType(String targetType) {
    this.targetType = targetType;
  }

}
