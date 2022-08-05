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

  @HopMetadataProperty(key = "target_field_name")
  private String targetFieldName;

  @HopMetadataProperty(key = "target_type")
  private String targetType;

  @HopMetadataProperty(key = "target_format")
  private String targetFormat;

  @HopMetadataProperty(key = "target_length")
  private String targetLength;

  @HopMetadataProperty(key = "target_precision")
  private String targetPrecision;


  public TargetField(String sourceField, String targetFieldName, String targetType, String targetFormat,
                     String targetLength, String targetPrecision) {
    this.sourceField = sourceField;
    this.targetFieldName = targetFieldName;
    this.targetType = targetType;
    this.targetFormat = targetFormat;
    this.targetLength = targetLength;
    this.targetPrecision = targetPrecision;
  }

  public TargetField(TargetField f) {
    this.sourceField = f.sourceField;
    this.targetFieldName = f.targetFieldName;
    this.targetType = f.targetType;
    this.targetFormat = f.targetFormat;
    this.targetLength = f.targetLength;
    this.targetPrecision = f.targetPrecision;
  }

  public IValueMeta createTargetValueMeta(IVariables variables) throws HopException {
    String name = variables.resolve(Const.NVL(targetFieldName, sourceField));
    int type = ValueMetaFactory.getIdForValueMeta(variables.resolve(targetType));
    int length = Const.toInt(variables.resolve(targetLength), -1);
    int precision = Const.toInt(variables.resolve(targetPrecision), -1);
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);
    valueMeta.setConversionMask(variables.resolve(targetFormat));
    return valueMeta;
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

  public String getTargetFormat() {
    return targetFormat;
  }

  public void setTargetFormat(String targetFormat) {
    this.targetFormat = targetFormat;
  }

  public String getTargetLength() {
    return targetLength;
  }

  public void setTargetLength(String targetLength) {
    this.targetLength = targetLength;
  }

  public String getTargetPrecision() {
    return targetPrecision;
  }

  public void setTargetPrecision(String targetPrecision) {
    this.targetPrecision = targetPrecision;
  }
}
