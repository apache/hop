/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.zip.ZipOutputStream;

import javax.xml.stream.XMLStreamWriter;


/**
 * @author Matt
 * @since 22-jan-2005
 */
public class XmlOutputData extends BaseTransformData implements ITransformData {
  public int splitnr;

  public Object[] headerrow;

  public int[] fieldnrs;

  public NumberFormat nf;

  public DecimalFormat df;

  public IRowMeta formatRowMeta;

  public IRowMeta outputRowMeta;

  public DecimalFormatSymbols dfs;

  public SimpleDateFormat daf;

  public DateFormatSymbols dafs;

  public ZipOutputStream zip;

  public XMLStreamWriter writer;

  public DecimalFormat defaultDecimalFormat;

  public DecimalFormatSymbols defaultDecimalFormatSymbols;

  public SimpleDateFormat defaultDateFormat;

  public DateFormatSymbols defaultDateFormatSymbols;

  public boolean OpenedNewFile;

  /**
   *
   */
  public XmlOutputData() {
    super();

    nf = NumberFormat.getInstance();
    df = (DecimalFormat) nf;
    dfs = new DecimalFormatSymbols();

    defaultDecimalFormat = (DecimalFormat) NumberFormat.getInstance();
    defaultDecimalFormatSymbols = new DecimalFormatSymbols();

    daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    defaultDateFormat = new SimpleDateFormat();
    defaultDateFormatSymbols = new DateFormatSymbols();

    OpenedNewFile = false;

  }

}
