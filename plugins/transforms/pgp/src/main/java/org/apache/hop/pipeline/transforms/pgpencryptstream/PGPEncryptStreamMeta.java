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

package org.apache.hop.pipeline.transforms.pgpencryptstream;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.pgpdecryptstream.PGPDecryptStream;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "PGPEncryptStream",
    image = "pgpencryptstream.svg",
    description = "i18n::PGPEncryptStream.Description",
    name = "i18n::PGPEncryptStream.Name",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Cryptography",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/pgpdecryptstream.html")
public class PGPEncryptStreamMeta extends BaseTransformMeta
    implements ITransformMeta<PGPDecryptStream, PGPEncryptStreamData> {
  private static final Class<?> PKG = PGPEncryptStreamMeta.class; // For Translator

  /** GPG location */
  private String gpgLocation;

  /** Key name */
  private String keyname;

  /** dynamic stream filed */
  private String streamfield;

  /** function result: new value name */
  private String resultFieldName;

  /** Flag: keyname is dynamic */
  private boolean keynameInField;

  /** keyname fieldname */
  private String keynameFieldName;

  public PGPEncryptStreamMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @deprecated - typo */
  @Deprecated
  public void setGPGPLocation(String value) {
    this.setGPGLocation(value);
  }

  public void setGPGLocation(String value) {
    this.gpgLocation = value;
  }

  public String getGPGLocation() {
    return gpgLocation;
  }

  /** @return Returns the streamfield. */
  public String getStreamField() {
    return streamfield;
  }

  /** @param streamfield The streamfield to set. */
  public void setStreamField(String streamfield) {
    this.streamfield = streamfield;
  }

  /** @return Returns the keynameFieldName. */
  public String getKeynameFieldName() {
    return keynameFieldName;
  }

  /** @param keynameFieldName The keynameFieldName to set. */
  public void setKeynameFieldName(String keynameFieldName) {
    this.keynameFieldName = keynameFieldName;
  }

  /** @return Returns the keynameInField. */
  public boolean isKeynameInField() {
    return keynameInField;
  }

  /** @param keynameInField The keynameInField to set. */
  public void setKeynameInField(boolean keynameInField) {
    this.keynameInField = keynameInField;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /** @param resultFieldName The resultfieldname to set. */
  public void setResultFieldName(String resultFieldName) {
    this.resultFieldName = resultFieldName;
  }

  /** @return Returns the keyname. */
  public String getKeyName() {
    return keyname;
  }

  /** @param keyname The keyname to set. */
  public void setKeyName(String keyname) {
    this.keyname = keyname;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  @Override
  public Object clone() {
    PGPEncryptStreamMeta retval = (PGPEncryptStreamMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultFieldName = "result";
    streamfield = null;
    keyname = null;
    gpgLocation = null;
    keynameInField = false;
    keynameFieldName = null;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output fields (String)
    if (!Utils.isEmpty(resultFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(resultFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    " + XmlHandler.addTagValue("gpglocation", gpgLocation));
    retval.append("    " + XmlHandler.addTagValue("keyname", keyname));
    retval.append("    " + XmlHandler.addTagValue("keynameInField", keynameInField));
    retval.append("    " + XmlHandler.addTagValue("keynameFieldName", keynameFieldName));
    retval.append("    " + XmlHandler.addTagValue("streamfield", streamfield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultFieldName));
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      gpgLocation = XmlHandler.getTagValue(transformNode, "gpglocation");
      keyname = XmlHandler.getTagValue(transformNode, "keyname");

      keynameInField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "keynameInField"));
      keynameFieldName = XmlHandler.getTagValue(transformNode, "keynameFieldName");
      streamfield = XmlHandler.getTagValue(transformNode, "streamfield");
      resultFieldName = XmlHandler.getTagValue(transformNode, "resultfieldname");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PGPEncryptStreamMeta.Exception.UnableToReadTransformMeta"),
          e);
    }
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
    CheckResult cr;
    String errorMessage = "";

    if (Utils.isEmpty(gpgLocation)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.GPGLocationMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.GPGLocationOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    if (!isKeynameInField()) {
      if (Utils.isEmpty(keyname)) {
        errorMessage =
            BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.KeyNameMissing");
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        errorMessage = BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.KeyNameOK");
        cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
    }
    if (Utils.isEmpty(resultFieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(streamfield)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.StreamFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.StreamFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "PGPEncryptStreamMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PGPEncryptStreamMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      PGPEncryptStreamData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new PGPEncryptStream(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public PGPEncryptStreamData getTransformData() {
    return new PGPEncryptStreamData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
