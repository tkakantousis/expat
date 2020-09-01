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

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatProjectMemberFacade extends ExpatAbstractFacade<ExpatProjectMember> {
  private static final String GET_PROJECT_TEAM_BY_PROJECT_ID = "SELECT t.project_id, t.team_member, t.added, " +
    "t.team_role, p.projectname, u.username FROM project_team as t JOIN project as p ON " +
    "project_id=id JOIN users as u ON team_member=email WHERE project_id=?";
  private Connection connection;
  
  public ExpatProjectMemberFacade(Class<ExpatProjectMember> entityClass)
    throws SQLException, ConfigurationException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatProjectMemberFacade(Class<ExpatProjectMember> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return "SELECT * FROM project_team";
  }
  
  @Override
  public String findByIdQuery() {
    return "SELECT * FROM project_team WHERE project_id = ? AND team_member = ?";
  }
  
  public List<ExpatProjectMember> findByProjectId(Integer projectId) throws IllegalAccessException, SQLException,
    InstantiationException {
    return this.findByQuery(GET_PROJECT_TEAM_BY_PROJECT_ID, projectId, JDBCType.INTEGER);
  }
}
