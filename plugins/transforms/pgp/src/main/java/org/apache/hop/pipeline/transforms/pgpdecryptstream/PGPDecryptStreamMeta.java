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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
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
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "PGPDecryptStream",
    image = "pgpdecryptstream.svg",
    description = "i18n::PGPDecryptStream.Description",
    name = "i18n::PGPDecryptStream.Name",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Cryptography",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/pgpdecryptstream.html")
public class PGPDecryptStreamMeta extends BaseTransformMeta
    implements ITransformMeta<PGPDecryptStream, PGPDecryptStreamData> {
  private static final Class<?> PKG = PGPDecryptStreamMeta.class; // For Translator

  /** GPG location */
  private String gpgLocation;

  /** passhrase */
  private String passhrase;

  /** Flag : passphrase from field */
  private boolean passphraseFromField;

  /** passphrase fieldname */
  private String passphraseFieldName;

  /** dynamic stream filed */
  private String streamfield;

  /** function result: new value name */
  private String resultfieldname;

  public PGPDecryptStreamMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void setGPGLocation(String gpgLocation) {
    this.gpgLocation = gpgLocation;
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

  /** @return Returns the passphraseFieldName. */
  public String getPassphraseFieldName() {
    return passphraseFieldName;
  }

  /** @param passphraseFieldName The passphraseFieldName to set. */
  public void setPassphraseFieldName(String passphraseFieldName) {
    this.passphraseFieldName = passphraseFieldName;
  }

  /** @return Returns the passphraseFromField. */
  public boolean isPassphraseFromField() {
    return passphraseFromField;
  }

  /** @param passphraseFromField The passphraseFromField to set. */
  public void setPassphraseFromField(boolean passphraseFromField) {
    this.passphraseFromField = passphraseFromField;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultFieldName to set */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @return Returns the passhrase. */
  public String getPassphrase() {
    return passhrase;
  }

  /** @param passhrase The passhrase to set. */
  public void setPassphrase(String passhrase) {
    this.passhrase = passhrase;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  @Override
  public Object clone() {
    PGPDecryptStreamMeta retval = (PGPDecryptStreamMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultfieldname = "result";
    streamfield = null;
    passhrase = null;
    gpgLocation = null;
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
    if (!Utils.isEmpty(resultfieldname)) {
      IValueMeta v = new ValueMetaString(variables.resolve(resultfieldname));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    " + XmlHandler.addTagValue("gpglocation", gpgLocation));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "passhrase", Encr.encryptPasswordIfNotUsingVariables(passhrase)));
    retval.append("    " + XmlHandler.addTagValue("streamfield", streamfield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    " + XmlHandler.addTagValue("passphraseFromField", passphraseFromField));
    retval.append("    " + XmlHandler.addTagValue("passphraseFieldName", passphraseFieldName));
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      gpgLocation = XmlHandler.getTagValue(transformNode, "gpglocation");
      passhrase =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "passhrase"));
      streamfield = XmlHandler.getTagValue(transformNode, "streamfield");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      passphraseFromField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "passphraseFromField"));
      passphraseFieldName = XmlHandler.getTagValue(transformNode, "passphraseFieldName");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.Exception.UnableToReadTransformMeta"),
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
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.GPGLocationOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    if (!isPassphraseFromField()) {
      // Check static pass-phrase
      if (Utils.isEmpty(passhrase)) {
        errorMessage =
            BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseMissing");
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.PassphraseOK");
        cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
    }
    if (Utils.isEmpty(resultfieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(streamfield)) {
      errorMessage =
          BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.StreamFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "PGPDecryptStreamMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PGPDecryptStreamMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      PGPDecryptStreamData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new PGPDecryptStream(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public PGPDecryptStreamData getTransformData() {
    return new PGPDecryptStreamData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
