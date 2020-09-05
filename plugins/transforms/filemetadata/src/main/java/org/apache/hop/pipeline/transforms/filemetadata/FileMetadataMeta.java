/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.filemetadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.Counter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
        id="FileMetadataPlugin",
        name="FileMetadata.Name.Default",
        image="icon.svg",
        description="FileMetadata.Name.Desc",
        i18nPackageName = "org.apache.hop.pipeline.transforms.filemetadata",
        categoryDescription = "i18n:org.apache.hop.pipeline.transforms:BaseStep.Category.Transform")
public class FileMetadataMeta extends BaseTransformMeta implements ITransformMeta<FileMetadata, FileMetadataData> {

//  public enum DetectionMethod {
//    FILE_FORMAT,          // delimited or fixed width?
//    DELIMITED_LAYOUT,     // delimiters, enclosure, skip header lines etc.
//    DELIMITED_FIELDS,     // fields and types in a delimited file
//    FIXED_LAYOUT,         // fixed layout, total record length, nr. of fields
//    FIXED_FIELDS          // fixed fields layout beginning, end
//  }

  /**
   * The PKG member is used when looking up internationalized strings.
   * The properties file with localized keys is expected to reside in
   * {the package of the class specified}/messages/messages_{locale}.properties
   */
  private static Class<?> PKG = FileMetadataMeta.class; // for i18n purposes


  /**
   * Stores the name of the file to examine
   */
  private String fileName = "";
  private String limitRows = "0";
  private String defaultCharset = "ISO-8859-1";

  // candidates for delimiters in delimited files
  private ArrayList<String> delimiterCandidates = new ArrayList<>(5);

  // candidates for enclosure characters in delimited files
  private ArrayList<String> enclosureCandidates = new ArrayList<>(5);


  /**
   * Constructor should call super() to make sure the base class has a chance to initialize properly.
   */
  public FileMetadataMeta() {
    super();
  }

  /**
   * This method is called every time a new step is created and should allocate/set the step configuration
   * to sensible defaults. The values set here will be used by Spoon when a new step is created.
   */
  public void setDefault() {
    fileName = "";
    limitRows = "10000";
    defaultCharset = "ISO-8859-1";

    delimiterCandidates.clear();
    delimiterCandidates.add("\t");
    delimiterCandidates.add(";");
    delimiterCandidates.add(",");

    enclosureCandidates.clear();
    enclosureCandidates.add("\"");
    enclosureCandidates.add("'");
  }


  /**
   * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
   * step meta object. Be sure to create proper deep copies if the step configuration is stored in
   * modifiable objects.
   * <p/>
   * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
   * a deep copy.
   *
   * @return a deep copy of this
   */
  public Object clone() {
    FileMetadataMeta copy = (FileMetadataMeta) super.clone();
    copy.setDelimiterCandidates(new ArrayList<>(this.delimiterCandidates));
    copy.setEnclosureCandidates(new ArrayList<>(this.enclosureCandidates));
    return copy;
  }

  @Override
  public ITransform createTransform(TransformMeta transformMeta, FileMetadataData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new FileMetadata(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public FileMetadataData getTransformData() {
    return new FileMetadataData();
  }

  public String getXML() throws HopValueException {

    StringBuilder buffer = new StringBuilder(800);

    buffer.append("    ").append(XmlHandler.addTagValue("fileName", fileName));
    buffer.append("    ").append(XmlHandler.addTagValue("limitRows", limitRows));
    buffer.append("    ").append(XmlHandler.addTagValue("defaultCharset", defaultCharset));

    for (String delimiterCandidate : delimiterCandidates) {
      buffer.append("      <delimiterCandidate>").append(Const.CR);
      buffer.append("        ").append(XmlHandler.addTagValue("candidate", delimiterCandidate));
      buffer.append("      </delimiterCandidate>").append(Const.CR);
    }

    for (String enclosureCandidate : enclosureCandidates) {
      buffer.append("      <enclosureCandidate>").append(Const.CR);
      buffer.append("        ").append(XmlHandler.addTagValue("candidate", enclosureCandidate));
      buffer.append("      </enclosureCandidate>").append(Const.CR);
    }

    return buffer.toString();
  }

  /**
   * This method is called by PDI when a step needs to load its configuration from XML.
   * <p/>
   * Please use org.pentaho.di.core.xml.XmlHandler to conveniently read from the
   * XML node passed in.
   *
   * @param stepnode  the XML node containing the configuration
   * @param databases the databases available in the transformation
   * @param counters  the counters available in the transformation
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws HopXmlException {

    try {
      setFileName(XmlHandler.getNodeValue(XmlHandler.getSubNode(stepnode, "fileName")));
      setLimitRows(XmlHandler.getNodeValue(XmlHandler.getSubNode(stepnode, "limitRows")));
      setDefaultCharset(XmlHandler.getNodeValue(XmlHandler.getSubNode(stepnode, "defaultCharset")));

      int nrDelimiters = XmlHandler.countNodes(stepnode, "delimiterCandidate");
      delimiterCandidates.clear();
      for (int i = 0; i < nrDelimiters; i++) {
        Node node = XmlHandler.getSubNodeByNr(stepnode, "delimiterCandidate", i);
        String candidate = XmlHandler.getTagValue(node, "candidate");
        delimiterCandidates.add(candidate);
      }

      int nrEnclosures = XmlHandler.countNodes(stepnode, "enclosureCandidate");
      enclosureCandidates.clear();
      for (int i = 0; i < nrEnclosures; i++) {
        Node node = XmlHandler.getSubNodeByNr(stepnode, "enclosureCandidate", i);
        String candidate = XmlHandler.getTagValue(node, "candidate");
        enclosureCandidates.add(candidate);
      }


    } catch (Exception e) {
      throw new HopXmlException("File metadata plugin unable to read step info from XML node", e);
    }

  }


  /**
   * This method is called to determine the changes the step is making to the row-stream.
   * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
   * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
   * row-stream.
   *
   * @param r        the row structure coming in to the step
   * @param origin   the name of the step making the changes
   * @param info     row structures of any info steps coming in
   * @param nextStep the description of a step this step is passing rows to
   * @param space    the variable space for resolving variables
   */
  public void getFields(IRowMeta r, String origin, IRowMeta[] info, TransformMeta nextStep, IVariables space) {

    r.addValueMeta(new ValueMetaString("charset"));
    r.addValueMeta(new ValueMetaString("delimiter"));
    r.addValueMeta(new ValueMetaString("enclosure"));
    r.addValueMeta(new ValueMetaInteger("field_count"));
    r.addValueMeta(new ValueMetaInteger("skip_header_lines"));
    r.addValueMeta(new ValueMetaInteger("skip_footer_lines"));
    r.addValueMeta(new ValueMetaBoolean("header_line_present"));
    r.addValueMeta(new ValueMetaString("name"));
    r.addValueMeta(new ValueMetaString("type"));
    r.addValueMeta(new ValueMetaInteger("length"));
    r.addValueMeta(new ValueMetaInteger("precision"));
    r.addValueMeta(new ValueMetaString("mask"));
    r.addValueMeta(new ValueMetaString("decimal_symbol"));
    r.addValueMeta(new ValueMetaString("grouping_symbol"));

  }

  public ArrayList<String> getDelimiterCandidates() {
    return delimiterCandidates;
  }

  public void setDelimiterCandidates(ArrayList<String> delimiterCandidates) {
    this.delimiterCandidates = delimiterCandidates;
  }

  public ArrayList<String> getEnclosureCandidates() {
    return enclosureCandidates;
  }

  public void setEnclosureCandidates(ArrayList<String> enclosureCandidates) {
    this.enclosureCandidates = enclosureCandidates;
  }

  public String getLimitRows() {
    return limitRows;
  }

  public void setLimitRows(String limitRows) {
    this.limitRows = limitRows;
  }

  public String getDefaultCharset() {
    return defaultCharset;
  }

  public void setDefaultCharset(String defaultCharset) {
    this.defaultCharset = defaultCharset;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

}
