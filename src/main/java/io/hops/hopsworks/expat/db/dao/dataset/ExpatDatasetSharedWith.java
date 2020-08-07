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
import java.util.Date;

public class ExpatDatasetSharedWith extends ExpatAbstractEntity<ExpatDatasetSharedWith> {

  private Integer id;
  private boolean accepted;
  private Date sharedOn;
  private Integer datasetId;
  private Integer project;
  private String permission;
  
  public ExpatDatasetSharedWith() {
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public boolean isAccepted() {
    return accepted;
  }
  
  public void setAccepted(boolean accepted) {
    this.accepted = accepted;
  }
  
  public Date getSharedOn() {
    return sharedOn;
  }
  
  public void setSharedOn(Date sharedOn) {
    this.sharedOn = sharedOn;
  }
  
  public Integer getDatasetId() {
    return datasetId;
  }
  
  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }
  
  public Integer getProject() {
    return project;
  }
  
  public void setProject(Integer project) {
    this.project = project;
  }
  
  public String getPermission() {
    return permission;
  }
  
  public void setPermission(String permission) {
    this.permission = permission;
  }
  
  @Override
  public ExpatDatasetSharedWith getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.accepted = resultSet.getBoolean("accepted");
    this.sharedOn = resultSet.getDate("shared_on");
    this.datasetId = resultSet.getInt("dataset");
    this.project = resultSet.getInt("project");
    this.permission = resultSet.getString("permission");
    return this;
  }
}
