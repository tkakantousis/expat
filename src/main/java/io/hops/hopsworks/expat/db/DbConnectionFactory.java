package io.hops.hopsworks.expat.db;

import com.zaxxer.hikari.HikariDataSource;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionFactory {

  private static HikariDataSource ds = null;

  private static void init() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();

    ds = new HikariDataSource();
    ds.setDriverClassName(config.getString(ExpatConf.DATABASE_DBMS_DRIVER_NAME,
        ExpatConf.DATABASE_DBMS_DRIVER_NAME_DEFAULT));
    ds.setJdbcUrl(config.getString(ExpatConf.DATABASE_URL));
    ds.setUsername(config.getString(ExpatConf.DATABASE_USER_KEY));
    ds.setPassword(config.getString(ExpatConf.DATABASE_PASSWORD_KEY));
  }

  public static Connection getConnection() throws ConfigurationException, SQLException {
    if (ds == null) {
      init();
    }
    return ds.getConnection();
  }
}
