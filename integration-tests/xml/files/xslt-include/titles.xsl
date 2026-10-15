<?xml version="1.0" encoding="UTF-8"?>
<!--

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-->
<!--
  Issue #8936: the import and the include are relative to this stylesheet, not to the
  working directory. The document() call is relative to the XML file that names it.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:import href="separator.xsl"/>
  <xsl:include href="line.xsl"/>
  <xsl:output method="text" encoding="UTF-8"/>
  <xsl:template match="/">
    <xsl:for-each select="catalog/cd">
      <xsl:call-template name="line">
        <xsl:with-param name="text" select="title"/>
      </xsl:call-template>
    </xsl:for-each>
    <xsl:for-each select="document(catalog/more/@href)/catalog/cd">
      <xsl:call-template name="line">
        <xsl:with-param name="text" select="title"/>
      </xsl:call-template>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
