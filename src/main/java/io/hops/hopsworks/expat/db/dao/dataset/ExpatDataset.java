/*
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */
package io.hops.hopsworks.expat.db.dao.dataset;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExpatDataset extends ExpatAbstractEntity<ExpatDataset> {
  private Integer id;
  private Long inodeId;
  private String name;
  private String description;
  private boolean searchable;
  private int publicDs;
  private String publicDsId;
  private String dsType;
  private Integer projectId;
  private Integer featureStoreId;
  private String permission;
  
  public ExpatDataset() {
  }
  
  public ExpatDataset(Integer id, Long inodeId, String name, String description, boolean searchable, int publicDs,
    String publicDsId, String dsType, Integer projectId, Integer featureStoreId, String permission) {
    this.id = id;
    this.inodeId = inodeId;
    this.name = name;
    this.description = description;
    this.searchable = searchable;
    this.publicDs = publicDs;
    this.publicDsId = publicDsId;
    this.dsType = dsType;
    this.projectId = projectId;
    this.featureStoreId = featureStoreId;
    this.permission = permission;
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public Long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getDescription() {
    return description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public boolean isSearchable() {
    return searchable;
  }
  
  public void setSearchable(boolean searchable) {
    this.searchable = searchable;
  }
  
  public int getPublicDs() {
    return publicDs;
  }
  
  public void setPublicDs(int publicDs) {
    this.publicDs = publicDs;
  }
  
  public String getPublicDsId() {
    return publicDsId;
  }
  
  public void setPublicDsId(String publicDsId) {
    this.publicDsId = publicDsId;
  }
  
  public String getDsType() {
    return dsType;
  }
  
  public void setDsType(String dsType) {
    this.dsType = dsType;
  }
  
  public Integer getProjectId() {
    return projectId;
  }
  
  public void setProjectId(Integer projectId) {
    this.projectId = projectId;
  }
  
  public Integer getFeatureStoreId() {
    return featureStoreId;
  }
  
  public void setFeatureStoreId(Integer featureStoreId) {
    this.featureStoreId = featureStoreId;
  }
  
  public String getPermission() {
    return permission;
  }
  
  public void setPermission(String permission) {
    this.permission = permission;
  }
  
  @Override
  public ExpatDataset getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.inodeId = resultSet.getLong("inode_id");
    this.name = resultSet.getString("inode_name");
    this.description = resultSet.getString("description");
    this.searchable = resultSet.getBoolean("searchable");
    this.publicDs = resultSet.getInt("public_ds");
    this.publicDsId = resultSet.getString("public_ds_id");
    this.dsType = resultSet.getString("dstype");
    this.projectId = resultSet.getInt("projectId");
    this.featureStoreId = resultSet.getInt("feature_store_id");
    this.permission = resultSet.getString("permission");
    return this;
  }
}
