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
package io.hops.hopsworks.expat.db.dao;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class ExpatAbstractFacade<E extends ExpatAbstractEntity> {
  private final Class<E> entityClass;
  
  protected ExpatAbstractFacade(Class<E> entityClass) {
    this.entityClass = entityClass;
  }
  
  public abstract Connection getConnection();
  
  public List<E> findAll() throws SQLException, IllegalAccessException, InstantiationException {
    Statement statement = getConnection().createStatement();
    ResultSet resultSet = null;
    List<E> resultList = new ArrayList<>();
    try {
      resultSet = statement.executeQuery(this.findAllQuery());
      while (resultSet.next()) {
        E entity = this.entityClass.newInstance();
        resultList.add((E) entity.getEntity(resultSet));
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
    }
    return resultList;
  }
  
  public E findById(Object id, JDBCType sqlType) throws SQLException, IllegalAccessException, InstantiationException {
    return findByCompositeId(new Object[]{id}, new JDBCType[]{sqlType});
  }
  
  public E findByCompositeId(Object[] ids, JDBCType[] sqlType) throws SQLException, IllegalAccessException,
    InstantiationException {
    List<E> resultList = findByQuery(this.findByIdQuery(), ids, sqlType);
    if (resultList.isEmpty()) {
      throw new IllegalStateException("No result found");
    }
    if (resultList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return resultList.get(0);
  }
  
  public List<E> findByQuery(String query, Object param, JDBCType sqlType)
    throws SQLException, IllegalAccessException, InstantiationException {
    return findByQuery(query, new Object[]{param}, new JDBCType[]{sqlType});
  }
  
  public List<E> findByQuery(String query, Object[] params, JDBCType[] sqlType)
    throws SQLException, IllegalAccessException, InstantiationException {
    ResultSet resultSet = null;
    List<E> resultList = new ArrayList<>();
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = getConnection().prepareStatement(query);
      for (int i = 0; i < params.length; i++) {
        preparedStatement.setObject(i, params[i], sqlType[i]);
      }
      resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        E entity = this.entityClass.newInstance();
        resultList.add((E) entity.getEntity(resultSet));
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    }
    return resultList;
  }
  
  public void update(String query, Object param, JDBCType sqlType) throws SQLException {
    update(query, new Object[]{param}, new JDBCType[]{sqlType});
  }
  
  public void update(String query, Object[] params, JDBCType[] sqlType) throws SQLException {
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = getConnection().prepareStatement(query);
      for (int i = 0; i < params.length; i++) {
        preparedStatement.setObject(i, params[i], sqlType[i]);
      }
      preparedStatement.execute();
      getConnection().commit();
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    }
  }
  
  public void closeConnection() throws SQLException {
    if (this.getConnection() != null) {
      this.getConnection().close();
    }
  }
  
  public abstract String findAllQuery();
  
  public abstract String findByIdQuery();
}
