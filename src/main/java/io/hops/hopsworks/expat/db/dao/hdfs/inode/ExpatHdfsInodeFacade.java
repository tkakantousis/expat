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
package io.hops.hopsworks.expat.db.dao.hdfs.inode;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;

public class ExpatHdfsInodeFacade extends ExpatAbstractFacade<ExpatHdfsInode> {
  private Connection connection;
  protected ExpatHdfsInodeFacade(Class<ExpatHdfsInode> entityClass) throws SQLException, ConfigurationException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatHdfsInodeFacade(Class<ExpatHdfsInode> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return "SELECT * FROM hops.hdfs_inodes";
  }
  
  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hops.hdfs_inodes WHERE id = ?";
  }
  
  public ExpatHdfsInode find(Long id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.BIGINT);
  }
}
