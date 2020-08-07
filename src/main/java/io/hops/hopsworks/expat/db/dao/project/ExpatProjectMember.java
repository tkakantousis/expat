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
package io.hops.hopsworks.expat.db.dao.project;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ExpatProjectMember extends ExpatAbstractEntity<ExpatProjectMember> {
  private Integer projectId;
  private String teamMember;
  private String teamRole;
  private Date timestamp;
  
  public ExpatProjectMember() {
  }
  
  public Integer getProjectId() {
    return projectId;
  }
  
  public void setProjectId(Integer projectId) {
    this.projectId = projectId;
  }
  
  public String getTeamMember() {
    return teamMember;
  }
  
  public void setTeamMember(String teamMember) {
    this.teamMember = teamMember;
  }
  
  public String getTeamRole() {
    return teamRole;
  }
  
  public void setTeamRole(String teamRole) {
    this.teamRole = teamRole;
  }
  
  public Date getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }
  
  @Override
  public ExpatProjectMember getEntity(ResultSet resultSet) throws SQLException {
    this.projectId = resultSet.getInt("project_id");
    this.teamMember = resultSet.getString("team_member");
    this.teamRole = resultSet.getString("team_role");
    this.timestamp = resultSet.getDate("added");
    return this;
  }
}
