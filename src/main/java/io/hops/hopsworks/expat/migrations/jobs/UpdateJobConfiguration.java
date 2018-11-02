package io.hops.hopsworks.expat.migrations.jobs;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.x509.GenerateUserCertificates;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdateJobConfiguration implements MigrateStep {

  private final static Logger LOGGER = Logger.getLogger(GenerateUserCertificates.class.getName());
  private final static String GET_ALL_JOB_CONFIGURATIONS = "SELECT id, json_config FROM hopsworks.jobs";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE hopsworks.jobs SET json_config = ? WHERE id = ?";
  protected Connection connection;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }


  @Override
  public void migrate() throws MigrationException, SQLException {
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    }

    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery(GET_ALL_JOB_CONFIGURATIONS);

    PreparedStatement updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
    while(resultSet.next()) {
      int id = resultSet.getInt(0);
      String jsonConfig = resultSet.getString(1);

    }



  }

  
  private String convertJSON(String oldConfig) {



    return null;
  }

  @Override
  public void rollback() throws RollbackException {

  }
}
