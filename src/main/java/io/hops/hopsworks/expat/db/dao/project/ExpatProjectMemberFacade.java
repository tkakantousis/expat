package io.hops.hopsworks.expat.db.dao.project;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetSharedWith;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatProjectMemberFacade extends ExpatAbstractFacade<ExpatProjectMember> {
  private static final String GET_PROJECT_TEAM_BY_PROJECT_ID = "SELECT * FROM project_team WHERE project_id=?";
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
