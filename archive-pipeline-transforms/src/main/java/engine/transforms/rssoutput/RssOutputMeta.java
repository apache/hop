/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.rssoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Output rows to RSS feed and create a file.
 *
 * @author Samatar
 * @since 6-nov-2007
 */

public class RssOutputMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = RssOutput.class; // for i18n purposes, needed by Translator!!

  private String channeltitle;
  private String channeldescription;
  private String channellink;
  private String channelpubdate;
  private String channelcopyright;
  private String channelimagetitle;
  private String channelimagelink;
  private String channelimageurl;
  private String channelimagedescription;
  private String channellanguage;
  private String channelauthor;

  private String itemtitle;
  private String itemdescription;
  private String itemlink;
  private String itempubdate;
  private String itemauthor;
  private String geopointlat;
  private String geopointlong;

  private boolean AddToResult;

  /**
   * The base name of the output file
   */
  private String fileName;

  /**
   * The file extention in case of a generated filename
   */
  private String extension;

  /**
   * Flag: add the transformnr in the filename
   */
  private boolean transformNrInFilename;

  /**
   * Flag: add the partition number in the filename
   */
  private boolean partNrInFilename;

  /**
   * Flag: add the date in the filename
   */
  private boolean dateInFilename;

  /**
   * Flag: add the time in the filename
   */
  private boolean timeInFilename;

  /**
   * Flag: create parent folder if needed
   */
  private boolean createparentfolder;

  /**
   * Rss version
   **/
  private String version;

  /**
   * Rss encoding
   **/
  private String encoding;

  /**
   * Flag : add image to RSS feed
   **/
  private boolean addimage;

  private boolean addgeorss;

  private boolean usegeorssgml;

  /**
   * The field that contain filename
   */
  private String filenamefield;

  /**
   * Flag : is filename defined in a field
   **/
  private boolean isfilenameinfield;

  /**
   * which fields do we use for Channel Custom ?
   */
  private String[] ChannelCustomFields;

  /**
   * add namespaces?
   */
  private String[] NameSpaces;

  private String[] NameSpacesTitle;

  /**
   * which fields do we use for getChannelCustomTags Custom ?
   */
  private String[] channelCustomTags;

  /**
   * which fields do we use for Item Custom Field?
   */
  private String[] ItemCustomFields;

  /**
   * which fields do we use for Item Custom tag ?
   */
  private String[] itemCustomTags;

  /**
   * create custom RSS ?
   */
  private boolean customrss;

  /**
   * display item tag in output ?
   */
  private boolean displayitem;

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {

    RssOutputMeta retval = (RssOutputMeta) super.clone();
    int nrFields = ChannelCustomFields.length;
    retval.allocate( nrFields );

    // Read custom channel fields
    System.arraycopy( ChannelCustomFields, 0, retval.ChannelCustomFields, 0, nrFields );
    System.arraycopy( channelCustomTags, 0, retval.channelCustomTags, 0, nrFields );

    // items
    int nritemfields = ItemCustomFields.length;
    retval.allocateitem( nritemfields );
    System.arraycopy( ItemCustomFields, 0, retval.ItemCustomFields, 0, nritemfields );
    System.arraycopy( itemCustomTags, 0, retval.itemCustomTags, 0, nritemfields );

    // Namespaces
    int nrNameSpaces = NameSpaces.length;
    retval.allocatenamespace( nrNameSpaces );
    System.arraycopy( NameSpacesTitle, 0, retval.NameSpacesTitle, 0, nrNameSpaces );
    System.arraycopy( NameSpaces, 0, retval.NameSpaces, 0, nrNameSpaces );

    return retval;

  }

  public void allocate( int nrFields ) {
    ChannelCustomFields = new String[ nrFields ];
    channelCustomTags = new String[ nrFields ];
  }

  public void allocateitem( int nrFields ) {
    ItemCustomFields = new String[ nrFields ];
    itemCustomTags = new String[ nrFields ];
  }

  public void allocatenamespace( int nrnamespaces ) {
    NameSpaces = new String[ nrnamespaces ];
    NameSpacesTitle = new String[ nrnamespaces ];
  }

  /**
   * @return Returns the version.
   */
  public String getVersion() {
    return version;
  }

  /**
   * @param version The version to set.
   */
  public void setVersion( String version ) {
    this.version = version;
  }

  /**
   * @return Returns the encoding.
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding The encoding to set.
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @return Returns the filenamefield.
   */
  public String getFileNameField() {
    return filenamefield;
  }

  /**
   * @param filenamefield The filenamefield to set.
   */
  public void setFileNameField( String filenamefield ) {
    this.filenamefield = filenamefield;
  }

  /**
   * @return Returns the extension.
   */
  public String getExtension() {
    return extension;
  }

  /**
   * @param extension The extension to set.
   */
  public void setExtension( String extension ) {
    this.extension = extension;
  }

  /**
   * @return Returns the fileName.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return Returns the transformNrInFilename.
   */
  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  /**
   * @param transformNrInFilename The transformNrInFilename to set.
   */
  public void setTransformNrInFilename( boolean transformNrInFilename ) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /**
   * @return Returns the timeInFilename.
   */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /**
   * @return Returns the dateInFilename.
   */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the Add to result filesname flag.
   */
  public boolean AddToResult() {
    return AddToResult;
  }

  /**
   * @param AddToResult The Add file to result to set.
   */
  public void setAddToResult( boolean AddToResult ) {
    this.AddToResult = AddToResult;
  }

  /**
   * @param customrss The custom RSS flag to set.
   */
  public void setCustomRss( boolean customrss ) {
    this.customrss = customrss;
  }

  /**
   * @return Returns the custom RSS flag.
   */
  public boolean isCustomRss() {
    return customrss;
  }

  /**
   * @param displayitem The display itema ta flag.
   */
  public void setDisplayItem( boolean displayitem ) {
    this.displayitem = displayitem;
  }

  /**
   * @return Returns the displayitem.
   */
  public boolean isDisplayItem() {
    return displayitem;
  }

  /**
   * @return Returns the addimage flag.
   */
  public boolean AddImage() {
    return addimage;
  }

  /**
   * @param addimage The addimage to set.
   */
  public void setAddImage( boolean addimage ) {
    this.addimage = addimage;
  }

  /**
   * @return Returns the addgeorss flag.
   */
  public boolean AddGeoRSS() {
    return addgeorss;
  }

  /**
   * @param addgeorss The addgeorss to set.
   */
  public void setAddGeoRSS( boolean addgeorss ) {
    this.addgeorss = addgeorss;
  }

  /**
   * @return Returns the addgeorss flag.
   */
  public boolean useGeoRSSGML() {
    return usegeorssgml;
  }

  /**
   * @param usegeorssgml The usegeorssgml to set.
   */
  public void setUseGeoRSSGML( boolean usegeorssgml ) {
    this.usegeorssgml = usegeorssgml;
  }

  /**
   * @return Returns the isfilenameinfield flag.
   */
  public boolean isFilenameInField() {
    return isfilenameinfield;
  }

  /**
   * @param isfilenameinfield The isfilenameinfield to set.
   */
  public void setFilenameInField( boolean isfilenameinfield ) {
    this.isfilenameinfield = isfilenameinfield;
  }

  /**
   * @return Returns the ChannelCustomFields (names in the stream).
   */
  public String[] getChannelCustomFields() {
    return ChannelCustomFields;
  }

  /**
   * @param ChannelCustomFields The ChannelCustomFields to set.
   */
  public void setChannelCustomFields( String[] ChannelCustomFields ) {
    this.ChannelCustomFields = ChannelCustomFields;
  }

  /**
   * @return Returns the NameSpaces.
   */
  public String[] getNameSpaces() {
    return NameSpaces;
  }

  /**
   * @param NameSpaces The NameSpaces to set.
   */
  public void setNameSpaces( String[] NameSpaces ) {
    this.NameSpaces = NameSpaces;
  }

  /**
   * @return Returns the NameSpaces.
   */
  public String[] getNameSpacesTitle() {
    return NameSpacesTitle;
  }

  /**
   * @param NameSpacesTitle The NameSpacesTitle to set.
   */
  public void setNameSpacesTitle( String[] NameSpacesTitle ) {
    this.NameSpacesTitle = NameSpacesTitle;
  }

  /**
   * @return Returns the getChannelCustomTags (names in the stream).
   */
  public String[] getChannelCustomTags() {
    return channelCustomTags;
  }

  /**
   * @param channelCustomTags The channelCustomTags to set.
   */
  public void setChannelCustomTags( String[] channelCustomTags ) {
    this.channelCustomTags = channelCustomTags;
  }

  /**
   * @return Returns the getChannelCustomTags (names in the stream).
   */
  public String[] getItemCustomTags() {
    return itemCustomTags;
  }

  /**
   * @param itemCustomTags The getChannelCustomTags to set.
   */
  public void setItemCustomTags( String[] itemCustomTags ) {
    this.itemCustomTags = itemCustomTags;
  }

  /**
   * @return Returns the ItemCustomFields (names in the stream).
   */
  public String[] getItemCustomFields() {
    return ItemCustomFields;
  }

  /**
   * @param value The ItemCustomFields to set.
   */
  public void setItemCustomFields( String[] value ) {
    this.ItemCustomFields = value;
  }

  /**
   * @return Returns the create parent folder flag.
   */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /**
   * @param createparentfolder The create parent folder flag to set.
   */
  public void setCreateParentFolder( boolean createparentfolder ) {
    this.createparentfolder = createparentfolder;
  }

  public boolean isPartNrInFilename() {
    return partNrInFilename;
  }

  public void setPartNrInFilename( boolean value ) {
    partNrInFilename = value;
  }

  public String[] getFiles( VariableSpace space ) throws HopTransformException {
    int copies = 1;
    int parts = 1;

    if ( transformNrInFilename ) {
      copies = 3;
    }

    if ( partNrInFilename ) {
      parts = 3;
    }

    int nr = copies * parts;
    if ( nr > 1 ) {
      nr++;
    }

    String[] retval = new String[ nr ];

    int i = 0;
    for ( int copy = 0; copy < copies; copy++ ) {
      for ( int part = 0; part < parts; part++ ) {
        retval[ i ] = buildFilename( space, copy );
        i++;
      }
    }
    if ( i < nr ) {
      retval[ i ] = "...";
    }

    return retval;
  }

  private String getFilename( VariableSpace space ) throws HopTransformException {
    FileObject file = null;
    try {
      file = HopVFS.getFileObject( space.environmentSubstitute( getFileName() ) );
      return HopVFS.getFilename( file );
    } catch ( Exception e ) {
      throw new HopTransformException( BaseMessages
        .getString( PKG, "RssOutput.Meta.ErrorGettingFile", getFileName() ), e );
    } finally {
      if ( file != null ) {
        try {
          file.close();
        } catch ( Exception e ) { /* Ignore */
        }
      }
    }
  }

  public String buildFilename( VariableSpace space, int transformnr ) throws HopTransformException {

    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = getFilename( space );

    Date now = new Date();

    if ( dateInFilename ) {
      daf.applyPattern( "yyyMMdd" );
      String d = daf.format( now );
      retval += "_" + d;
    }
    if ( timeInFilename ) {
      daf.applyPattern( "HHmmss" );
      String t = daf.format( now );
      retval += "_" + t;
    }
    if ( transformNrInFilename ) {
      retval += "_" + transformnr;
    }

    if ( extension != null && extension.length() != 0 ) {
      retval += "." + extension;
    }

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      displayitem = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "displayitem" ) );
      customrss = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "customrss" ) );
      channeltitle = XMLHandler.getTagValue( transformNode, "channel_title" );
      channeldescription = XMLHandler.getTagValue( transformNode, "channel_description" );
      channellink = XMLHandler.getTagValue( transformNode, "channel_link" );
      channelpubdate = XMLHandler.getTagValue( transformNode, "channel_pubdate" );
      channelcopyright = XMLHandler.getTagValue( transformNode, "channel_copyright" );

      channelimagetitle = XMLHandler.getTagValue( transformNode, "channel_image_title" );
      channelimagelink = XMLHandler.getTagValue( transformNode, "channel_image_link" );
      channelimageurl = XMLHandler.getTagValue( transformNode, "channel_image_url" );
      channelimagedescription = XMLHandler.getTagValue( transformNode, "channel_image_description" );
      channellanguage = XMLHandler.getTagValue( transformNode, "channel_language" );
      channelauthor = XMLHandler.getTagValue( transformNode, "channel_author" );

      version = XMLHandler.getTagValue( transformNode, "version" );
      encoding = XMLHandler.getTagValue( transformNode, "encoding" );

      addimage = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addimage" ) );

      // Items ...
      itemtitle = XMLHandler.getTagValue( transformNode, "item_title" );
      itemdescription = XMLHandler.getTagValue( transformNode, "item_description" );
      itemlink = XMLHandler.getTagValue( transformNode, "item_link" );
      itempubdate = XMLHandler.getTagValue( transformNode, "item_pubdate" );
      itemauthor = XMLHandler.getTagValue( transformNode, "item_author" );

      addgeorss = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "addgeorss" ) );
      usegeorssgml = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "usegeorssgml" ) );
      geopointlat = XMLHandler.getTagValue( transformNode, "geopointlat" );
      geopointlong = XMLHandler.getTagValue( transformNode, "geopointlong" );

      filenamefield = XMLHandler.getTagValue( transformNode, "file", "filename_field" );
      fileName = XMLHandler.getTagValue( transformNode, "file", "name" );

      isfilenameinfield =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "is_filename_in_field" ) );
      createparentfolder =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "create_parent_folder" ) );
      extension = XMLHandler.getTagValue( transformNode, "file", "extention" );
      transformNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "split" ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "haspartno" ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_date" ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_time" ) );
      AddToResult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "AddToResult" ) );

      Node keys = XMLHandler.getSubNode( transformNode, "fields" );
      // Custom Channel fields
      int nrchannelfields = XMLHandler.countNodes( keys, "channel_custom_fields" );
      allocate( nrchannelfields );

      for ( int i = 0; i < nrchannelfields; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( keys, "channel_custom_fields", i );
        channelCustomTags[ i ] = XMLHandler.getTagValue( knode, "tag" );
        ChannelCustomFields[ i ] = XMLHandler.getTagValue( knode, "field" );
      }
      // Custom Item fields
      int nritemfields = XMLHandler.countNodes( keys, "item_custom_fields" );
      allocateitem( nritemfields );

      for ( int i = 0; i < nritemfields; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( keys, "item_custom_fields", i );
        itemCustomTags[ i ] = XMLHandler.getTagValue( knode, "tag" );
        ItemCustomFields[ i ] = XMLHandler.getTagValue( knode, "field" );
      }
      // NameSpaces
      Node keysNameSpaces = XMLHandler.getSubNode( transformNode, "namespaces" );
      int nrnamespaces = XMLHandler.countNodes( keysNameSpaces, "namespace" );
      allocatenamespace( nrnamespaces );
      for ( int i = 0; i < nrnamespaces; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( keysNameSpaces, "namespace", i );
        NameSpacesTitle[ i ] = XMLHandler.getTagValue( knode, "namespace_tag" );
        NameSpaces[ i ] = XMLHandler.getTagValue( knode, "namespace_value" );
      }

    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void setDefault() {
    displayitem = true;
    customrss = false;
    channeltitle = null;
    channeldescription = null;
    channellink = null;
    channelpubdate = null;
    channelcopyright = null;
    channelimagetitle = null;
    channelimagelink = null;
    channelimageurl = null;
    channelimagedescription = null;
    channellanguage = null;
    channelauthor = null;
    createparentfolder = false;
    isfilenameinfield = false;
    version = "rss_2.0";
    encoding = "iso-8859-1";
    filenamefield = null;
    isfilenameinfield = false;

    // Items ...
    itemtitle = null;
    itemdescription = null;
    itemlink = null;
    itempubdate = null;
    itemauthor = null;
    geopointlat = null;
    geopointlong = null;
    int nrchannelfields = 0;
    allocate( nrchannelfields );
    // channel custom fields
    for ( int i = 0; i < nrchannelfields; i++ ) {
      ChannelCustomFields[ i ] = "field" + i;
      channelCustomTags[ i ] = "tag" + i;
    }

    int nritemfields = 0;
    allocateitem( nritemfields );
    // Custom Item Fields
    for ( int i = 0; i < nritemfields; i++ ) {
      ItemCustomFields[ i ] = "field" + i;
      itemCustomTags[ i ] = "tag" + i;
    }
    // Namespaces
    int nrnamespaces = 0;
    allocatenamespace( nrnamespaces );
    // Namespaces
    for ( int i = 0; i < nrnamespaces; i++ ) {
      NameSpacesTitle[ i ] = "namespace_title" + i;
      NameSpaces[ i ] = "namespace" + i;
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "displayitem", displayitem ) );
    retval.append( "    " + XMLHandler.addTagValue( "customrss", customrss ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_title", channeltitle ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_description", channeldescription ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_link", channellink ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_pubdate", channelpubdate ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_copyright", channelcopyright ) );

    retval.append( "    " + XMLHandler.addTagValue( "channel_image_title", channelimagetitle ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_image_link", channelimagelink ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_image_url", channelimageurl ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_image_description", channelimagedescription ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_language", channellanguage ) );
    retval.append( "    " + XMLHandler.addTagValue( "channel_author", channelauthor ) );

    retval.append( "    " + XMLHandler.addTagValue( "version", version ) );
    retval.append( "    " + XMLHandler.addTagValue( "encoding", encoding ) );

    retval.append( "    " + XMLHandler.addTagValue( "addimage", addimage ) );

    // Items ...

    retval.append( "    " + XMLHandler.addTagValue( "item_title", itemtitle ) );
    retval.append( "    " + XMLHandler.addTagValue( "item_description", itemdescription ) );
    retval.append( "    " + XMLHandler.addTagValue( "item_link", itemlink ) );
    retval.append( "    " + XMLHandler.addTagValue( "item_pubdate", itempubdate ) );
    retval.append( "    " + XMLHandler.addTagValue( "item_author", itemauthor ) );
    retval.append( "    " + XMLHandler.addTagValue( "addgeorss", addgeorss ) );
    retval.append( "    " + XMLHandler.addTagValue( "usegeorssgml", usegeorssgml ) );
    retval.append( "    " + XMLHandler.addTagValue( "geopointlat", geopointlat ) );
    retval.append( "    " + XMLHandler.addTagValue( "geopointlong", geopointlong ) );

    retval.append( "    <file>" + Const.CR );
    retval.append( "      " + XMLHandler.addTagValue( "filename_field", filenamefield ) );
    retval.append( "      " + XMLHandler.addTagValue( "name", fileName ) );
    retval.append( "      " + XMLHandler.addTagValue( "extention", extension ) );
    retval.append( "      " + XMLHandler.addTagValue( "split", transformNrInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "haspartno", partNrInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "add_date", dateInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "add_time", timeInFilename ) );
    retval.append( "      " + XMLHandler.addTagValue( "is_filename_in_field", isfilenameinfield ) );
    retval.append( "      " + XMLHandler.addTagValue( "create_parent_folder", createparentfolder ) );
    retval.append( "    " + XMLHandler.addTagValue( "addtoresult", AddToResult ) );
    retval.append( "      </file>" + Const.CR );

    retval.append( "      <fields>" ).append( Const.CR );
    for ( int i = 0; i < ChannelCustomFields.length; i++ ) {
      retval.append( "        <channel_custom_fields>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "tag", channelCustomTags[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "field", ChannelCustomFields[ i ] ) );
      retval.append( "        </channel_custom_fields>" ).append( Const.CR );
    }
    for ( int i = 0; i < ItemCustomFields.length; i++ ) {
      retval.append( "        <Item_custom_fields>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "tag", itemCustomTags[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "field", ItemCustomFields[ i ] ) );
      retval.append( "        </Item_custom_fields>" ).append( Const.CR );
    }
    retval.append( "      </fields>" ).append( Const.CR );

    retval.append( "      <namespaces>" ).append( Const.CR );
    for ( int i = 0; i < NameSpaces.length; i++ ) {
      retval.append( "        <namespace>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "namespace_tag", NameSpacesTitle[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "namespace_value", NameSpaces[ i ] ) );
      retval.append( "        </namespace>" ).append( Const.CR );
    }
    retval.append( "      </namespaces>" ).append( Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {

    CheckResult cr;

    // String error_message = "";
    // boolean error_found = false;
    // OK, we have the table fields.
    // Now see what we can find as previous transform...
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "RssOutputMeta.CheckResult.FieldsReceived", "" + prev.size() ), transformMeta );
      remarks.add( cr );

      // Starting from prev...
      /*
       * for (int i=0;i<prev.size();i++) { Value pv = prev.getValue(i); int idx = r.searchValueIndex(pv.getName()); if
       * (idx<0) { error_message+="\t\t"+pv.getName()+" ("+pv.getTypeDesc()+")"+Const.CR; error_found=true; } } if
       * (error_found) { error_message=BaseMessages.getString(PKG, "RssOutputMeta.CheckResult.FieldsNotFoundInOutput",
       * error_message);
       *
       * cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta); remarks.add(cr); } else { cr =
       * new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG,
       * "RssOutputMeta.CheckResult.AllFieldsFoundInOutput"), transformMeta); remarks.add(cr); }
       */

      // Starting from table fields in r...
      /*
       * for (int i=0;i<r.size();i++) { Value rv = r.getValue(i); int idx = prev.searchValueIndex(rv.getName()); if
       * (idx<0) { error_message+="\t\t"+rv.getName()+" ("+rv.getTypeDesc()+")"+Const.CR; error_found=true; } } if
       * (error_found) { error_message=BaseMessages.getString(PKG, "RssOutputMeta.CheckResult.FieldsNotFound",
       * error_message);
       *
       * cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, error_message, transformMeta); remarks.add(cr); } else { cr =
       * new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG,
       * "RssOutputMeta.CheckResult.AllFieldsFound"), transformMeta); remarks.add(cr); }
       */
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "RssOutputMeta.CheckResult.NoFields" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "RssOutputMeta.CheckResult.ExpectedInputOk" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "RssOutputMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public TransformDataInterface getTransformData() {
    return new RssOutputData();
  }

  /**
   * @return the channeltitle
   */
  public String getChannelTitle() {
    return channeltitle;
  }

  /**
   * @return the channeldescription
   */
  public String getChannelDescription() {
    return channeldescription;
  }

  /**
   * @return the channellink
   */
  public String getChannelLink() {
    return channellink;
  }

  /**
   * @return the channelpubdate
   */
  public String getChannelPubDate() {
    return channelpubdate;
  }

  /**
   * @return the channelimagelink
   */
  public String getChannelImageLink() {
    return channelimagelink;
  }

  /**
   * @return the channelimageurl
   */
  public String getChannelImageUrl() {
    return channelimageurl;
  }

  /**
   * @return the channelimagedescription
   */
  public String getChannelImageDescription() {
    return channelimagedescription;
  }

  /**
   * @return the channelimagetitle
   */
  public String getChannelImageTitle() {
    return channelimagetitle;
  }

  /**
   * @return the channellanguage
   */
  public String getChannelLanguage() {
    return channellanguage;
  }

  /**
   * @return the channelauthor
   */
  public String getChannelAuthor() {
    return channelauthor;
  }

  /**
   * @param channelauthor the channelauthor to set
   */
  public void setChannelAuthor( String channelauthor ) {
    this.channelauthor = channelauthor;
  }

  /**
   * @param channeltitle the channeltitle to set
   */
  public void setChannelTitle( String channeltitle ) {
    this.channeltitle = channeltitle;
  }

  /**
   * @param channellink the channellink to set
   */
  public void setChannelLink( String channellink ) {
    this.channellink = channellink;
  }

  /**
   * @param channelpubdate the channelpubdate to set
   */
  public void setChannelPubDate( String channelpubdate ) {
    this.channelpubdate = channelpubdate;
  }

  /**
   * @param channelimagetitle the channelimagetitle to set
   */
  public void setChannelImageTitle( String channelimagetitle ) {
    this.channelimagetitle = channelimagetitle;
  }

  /**
   * @param channelimagelink the channelimagelink to set
   */
  public void setChannelImageLink( String channelimagelink ) {
    this.channelimagelink = channelimagelink;
  }

  /**
   * @param channelimageurl the channelimageurl to set
   */
  public void setChannelImageUrl( String channelimageurl ) {
    this.channelimageurl = channelimageurl;
  }

  /**
   * @param channelimagedescription the channelimagedescription to set
   */
  public void setChannelImageDescription( String channelimagedescription ) {
    this.channelimagedescription = channelimagedescription;
  }

  /**
   * @param channellanguage the channellanguage to set
   */
  public void setChannelLanguage( String channellanguage ) {
    this.channellanguage = channellanguage;
  }

  /**
   * @param channeldescription the channeldescription to set
   */
  public void setChannelDescription( String channeldescription ) {
    this.channeldescription = channeldescription;
  }

  /**
   * @return the itemtitle
   */
  public String getItemTitle() {
    return itemtitle;
  }

  /**
   * @return the geopointlat
   */
  public String getGeoPointLat() {
    return geopointlat;
  }

  /**
   * @param geopointlat the geopointlat to set
   */
  public void setGeoPointLat( String geopointlat ) {
    this.geopointlat = geopointlat;
  }

  /**
   * @return the geopointlong
   */
  public String getGeoPointLong() {
    return geopointlong;
  }

  /**
   * @param geopointlong the geopointlong to set
   */
  public void setGeoPointLong( String geopointlong ) {
    this.geopointlong = geopointlong;
  }

  /**
   * @return the itemdescription
   */
  public String getItemDescription() {
    return itemdescription;
  }

  /**
   * @return the itemlink
   */
  public String getItemLink() {
    return itemlink;
  }

  /**
   * @return the itempubdate
   */
  public String getItemPubDate() {
    return itempubdate;
  }

  /**
   * @return the itemauthor
   */
  public String getItemAuthor() {
    return itemauthor;
  }

  /**
   * @param itemtitle the itemtitle to set
   */
  public void setItemTitle( String itemtitle ) {
    this.itemtitle = itemtitle;
  }

  /**
   * @param itemdescription the itemdescription to set
   */
  public void setItemDescription( String itemdescription ) {
    this.itemdescription = itemdescription;
  }

  /**
   * @param itemlink the itemlink to set
   */
  public void setItemLink( String itemlink ) {
    this.itemlink = itemlink;
  }

  /**
   * @param itempubdate the itempubdate to set
   */
  public void setItemPubDate( String itempubdate ) {
    this.itempubdate = itempubdate;
  }

  /**
   * @param itemauthor the itemauthor to set
   */
  public void setItemAuthor( String itemauthor ) {
    this.itemauthor = itemauthor;
  }

  /**
   * @return channelcopyrightt
   */
  public String getChannelCopyright() {
    return channelcopyright;
  }

  /**
   * @param channelcopyright the channelcopyright to set
   */
  public void setChannelCopyright( String channelcopyright ) {
    this.channelcopyright = channelcopyright;
  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new RssOutput( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }
}
