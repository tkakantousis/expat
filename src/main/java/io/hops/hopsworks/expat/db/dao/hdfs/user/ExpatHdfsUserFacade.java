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
package io.hops.hopsworks.expat.db.dao.hdfs.user;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatHdfsUserFacade extends ExpatAbstractFacade<ExpatHdfsUser> {
  private static final String FIND_BY_NAME = "SELECT * FROM hops.hdfs_users WHERE name = ?";
  private static final String FIND_ALL_IN_GROUP = "SELECT * FROM hops.hdfs_users JOIN hops.hdfs_users_groups ON " +
    "id=user_id WHERE group_id = ?";
  private Connection connection;
  protected ExpatHdfsUserFacade(Class<ExpatHdfsUser> entityClass) {
    super(entityClass);
  }
  
  public ExpatHdfsUserFacade(Class<ExpatHdfsUser> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return "SELECT * FROM hops.hdfs_users";
  }
  
  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hops.hdfs_users WHERE id = ?";
  }
  
  public ExpatHdfsUser findByName(String name) throws IllegalAccessException, SQLException, InstantiationException {
    List<ExpatHdfsUser> hdfsGroupList = this.findByQuery(FIND_BY_NAME, name, JDBCType.VARCHAR);
    if (hdfsGroupList.isEmpty()) {
      throw new IllegalStateException("No result found");
    }
    if (hdfsGroupList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return hdfsGroupList.get(0);
  }
  
  public ExpatHdfsUser find(Integer id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.INTEGER);
  }
  
  public List<ExpatHdfsUser> getUsersInGroup(ExpatHdfsGroup group) throws IllegalAccessException, SQLException,
    InstantiationException {
    List<ExpatHdfsUser> users = this.findByQuery(FIND_ALL_IN_GROUP, group.getId(), JDBCType.BIGINT);
    return users;
  }
}
