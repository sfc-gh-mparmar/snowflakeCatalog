/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.snowflake.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SnowflakeTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeTableOperations.class);
  private final String catalogName;

  private FileIO fileIO;
  private final TableIdentifier tableIdentifier;

  private Object configuration;

  private final Map<String, String> catalogProperties;

  protected SnowflakeTableOperations(
      Map<String, String> properties,
      String catalogName,
      Object configs,
      TableIdentifier tableIdentifier) {
    this.catalogProperties = properties;
    this.catalogName = catalogName;
    this.configuration = configs;
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  public void doRefresh() {
    Map<String, String> table;

    String location = null;
    try {
      location = getTableMetadataLocation();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting table from catalog
      throw new UncheckedSQLException(
          e, "Failed to get table %s from catalog %s", tableIdentifier, catalogName);
    }

    if (location.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Failed to load table %s from catalog %s: dropped by another process",
            tableIdentifier, catalogName);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = location;
    Preconditions.checkState(
        newMetadataLocation != null,
        "Invalid table %s: metadata location is null",
        tableIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);
  }

  static String getMetadataLocationFromJson(String json) {
    return JsonUtil.parse(json, SnowflakeTableOperations::getMetadataLocationFromJson);
  }

  static String getMetadataLocationFromJson(JsonNode json) {
    return JsonUtil.getString(SnowflakeUtils.METADATA_LOCATION, json);
  }

  @Override
  public FileIO io() {

    String fileIOImpl = SnowflakeUtils.DEFAULT_FILE_IO_IMPL;

    if (null != catalogProperties.get(CatalogProperties.FILE_IO_IMPL)) {
      fileIOImpl = catalogProperties.get(CatalogProperties.FILE_IO_IMPL);
    }

    return CatalogUtil.loadFileIO(fileIOImpl, catalogProperties, configuration);
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }

  private String getTableMetadataLocation()
      throws UncheckedSQLException, SQLException, InterruptedException {

    Connection connection =
        DriverManager.getConnection(catalogProperties.get(CatalogProperties.URI));
    Statement st = connection.createStatement();
    ResultSet results =
        st.executeQuery(String.format(SnowflakeUtils.METADATA_LOCATION_QUERY, tableName()));

    String locationJson = null;
    while (results.next()) {
      locationJson = results.getString(1);
    }
    results.close();
    st.close();

    return getMetadataLocationFromJson(locationJson);
  }
}
