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

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatDatasetFacade extends ExpatAbstractFacade<ExpatDataset> {
  private static final String GET_ALL_DATASETS_IN_PROJECT = "SELECT * FROM dataset WHERE projectId = ?";
  private final static String UPDATE_DATASET_PERMISSION = "UPDATE dataset SET permission = ? WHERE id = ?";
  private Connection connection;
  public ExpatDatasetFacade(Class<ExpatDataset> entityClass) throws SQLException, ConfigurationException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatDatasetFacade(Class<ExpatDataset> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return "SELECT * FROM dataset";
  }
  
  @Override
  public String findByIdQuery() {
    return "SELECT * FROM dataset WHERE id = ?";
  }
  
  public ExpatDataset find(Integer id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.INTEGER);
  }
  
  public List<ExpatDataset> findByProjectId(Integer projectId) throws IllegalAccessException, SQLException,
    InstantiationException {
    return this.findByQuery(GET_ALL_DATASETS_IN_PROJECT, projectId, JDBCType.INTEGER);
  }
  
  public void updatePermission(Integer id, String permission) throws SQLException {
    this.update(UPDATE_DATASET_PERMISSION, new Object[]{id, permission},
      new JDBCType[]{JDBCType.INTEGER, JDBCType.VARCHAR});
  }
}
