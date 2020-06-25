package org.apache.hop.core.search;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.IPluginType;

import java.util.Map;

public class SearchableAnalyserPluginType extends BasePluginType<SearchableAnalyserPlugin> implements IPluginType<SearchableAnalyserPlugin> {

  private static SearchableAnalyserPluginType pluginType;

  private SearchableAnalyserPluginType() {
    super( SearchableAnalyserPlugin.class, "SEARCH_ANALYSER", "SearchAnalyser" );
    populateFolders( "search-analysers" );
  }

  public static SearchableAnalyserPluginType getInstance() {
    if ( pluginType == null ) {
      pluginType = new SearchableAnalyserPluginType();
    }
    return pluginType;
  }

  protected void registerPluginJars() throws HopPluginException {
    super.registerPluginJars();
  }

  @Override
  public String getXmlPluginFile() {
    return Const.XML_FILE_HOP_SEARCH_ANALYSER_PLUGINS;
  }

  @Override
  public String getMainTag() {
    return "search-analysers";
  }

  @Override
  public String getSubTag() {
    return "search-analyser";
  }

  @Override
  protected String getPath() {
    return "./";
  }

  public String[] getNaturalCategoriesOrder() {
    return new String[ 0 ];
  }

  @Override
  protected String extractCategory( SearchableAnalyserPlugin annotation ) {
    return "";
  }

  @Override
  protected String extractID( SearchableAnalyserPlugin annotation ) {
    return annotation.id();
  }

  @Override
  protected String extractName( SearchableAnalyserPlugin annotation ) {
    return annotation.name();
  }

  @Override
  protected String extractDesc( SearchableAnalyserPlugin annotation ) {
    return annotation.description();
  }

  @Override
  protected String extractImageFile( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( SearchableAnalyserPlugin annotation ) {
    return false;
  }

  @Override
  protected String extractI18nPackageName( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, SearchableAnalyserPlugin annotation ) {
  }

  @Override
  protected String extractDocumentationUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractCasesUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractForumUrl( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractSuggestion( SearchableAnalyserPlugin annotation ) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup( SearchableAnalyserPlugin annotation ) {
    return null;
  }
}

