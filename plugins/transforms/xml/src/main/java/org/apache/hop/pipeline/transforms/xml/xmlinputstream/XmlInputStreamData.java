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

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import java.io.InputStream;
import java.util.Map;

/**
 * @author Jens Bleuel
 * @since 2011-01-13
 */
public class XmlInputStreamData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public IRowMeta finalOutputRowMeta;

  public XMLInputFactory staxInstance;

  public FileObject fileObject;
  public InputStream inputStream;
  public XMLEventReader xmlEventReader;

  // from meta data but replaced by variables
  public String[] filenames;
  public int filenr;
  public long nrRowsToSkip;
  public long rowLimit;
  public String encoding;
  public int previousFieldsNumber = 0;
  public Map<String, Object[]> inputDataRows;
  public Object[] currentInputRow;

  // runtime data
  public Long rowNumber;
  public int elementLevel;
  public Long elementID;
  public Long[] elementLevelID;
  public Long[] elementParentID;
  public String[] elementName;
  public String[] elementPath;

  // positions of fields in the row (-1: field is not included in the stream)
  public int pos_xmlFilename;
  public int pos_xmlRow_number;
  public int pos_xml_dataType_numeric;
  public int pos_xml_dataTypeDescription;
  public int pos_xml_location_line;
  public int pos_xml_locationColumn;
  public int pos_xml_element_id;
  public int pos_xml_parent_element_id;
  public int pos_xml_element_level;
  public int pos_xml_path;
  public int pos_xml_parent_path;
  public int pos_xml_dataName;
  public int pos_xml_dataValue;

  public XmlInputStreamData() {
    super();
  }
}
