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

package org.apache.hop.pipeline.transforms.rssinput;

import it.sauronsoftware.feed4j.bean.Feed;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Samatar Hassan
 * @since 13-10-2007
 */
public class RssInputData extends BaseTransformData implements ITransformData {
  public String thisline, nextline, lastline;
  public Object[] previousRow;
  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public int nr_repeats;

  public NumberFormat nf;
  public DecimalFormat df;
  public DecimalFormatSymbols dfs;
  public SimpleDateFormat daf;
  public DateFormatSymbols dafs;
  public Date readfromdatevalide;

  public long rownr;
  public int urlnr;
  public int urlsize;
  public String currenturl;
  public boolean last_url;
  public int itemssize;
  public int itemsnr;

  public String PLUGIN_VERSION;

  public int indexOfUrlField;
  public int totalpreviousfields;
  public int nrInputFields;
  public Feed feed;
  public Object[] readrow;

  public RssInputData() {
    super();

    thisline = null;
    nextline = null;
    nf = NumberFormat.getInstance();
    df = (DecimalFormat) nf;
    dfs = new DecimalFormatSymbols();
    daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    nr_repeats = 0;
    previousRow = null;
    readfromdatevalide = new Date();

    PLUGIN_VERSION = "1.1";

    indexOfUrlField = -1;
    totalpreviousfields = 0;
    urlsize = 0;
    currenturl = null;
    last_url = false;
    itemssize = 0;
    itemsnr = 0;
    feed = null;
    readrow = null;
  }
}
