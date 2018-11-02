package io.hops.hopsworks.expat.migrations.jobs;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.json.JSONObject;

import java.sql.*;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdateJobConfiguration implements MigrateStep {

  private final static Logger LOGGER = Logger.getLogger(UpdateJobConfiguration.class.getName());
  private final static String GET_ALL_JOB_CONFIGURATIONS = "SELECT id, json_config FROM jobs";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE jobs SET json_config = ? WHERE id = ?";
  protected Connection connection;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }

  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    }

    try {
      connection.setAutoCommit(false);
      Statement stmt = connection.createStatement();
      ResultSet allJobsResultSet = stmt.executeQuery(GET_ALL_JOB_CONFIGURATIONS);

      PreparedStatement updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (allJobsResultSet.next()) {
        int id = allJobsResultSet.getInt(1);
        String oldConfig = allJobsResultSet.getString(2);

        String newConfig = convertJSON(oldConfig);

        updateJSONConfigStmt.setString(1, newConfig);
        updateJSONConfigStmt.setInt(2, id);
        updateJSONConfigStmt.addBatch();
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
  }

  //This function converts an old jobConfig to the new format
  private String convertJSON(String oldConfig) {

    JSONObject config = new JSONObject(oldConfig);

    renameIfKeyExists(config, "type", "jobType");

    if(config.get("jobType").equals("SPARK") || config.get("jobType").equals("PYSPARK")) {
      addKeyValue(config, "type", "sparkJobConfiguration");
    } else {
      renameIfKeyExists(config, "jobType", "type");
      return oldConfig.toString();
    }

    //These do not exist in new Config, drop them
    removeKeyIfExists(config, "HISTORYSERVER");
    removeKeyIfExists(config, "PYSPARK_PYTHON");
    removeKeyIfExists(config, "PYLIB");
    removeKeyIfExists(config,"DYNEXECSMAX");
    removeKeyIfExists(config,"DYNEXECSMIN");

    //These were renamed
    renameIfKeyExists(config, "JARPATH", "appPath");
    renameIfKeyExists(config, "ARGS", "args");
    renameIfKeyExists(config, "APPNAME", "appName");
    renameIfKeyExists(config, "MAINCLASS", "mainClass");
    renameIfKeyExists(config, "PROPERTIES", "properties");
    renameIfKeyExists(config, "QUEUE", "queue");

    renameIfKeyExists(config, "AMMEM", "amMemory");
    renameIfKeyExists(config, "AMCORS", "amVCores");

    renameIfKeyExists(config, "EXECMEM", "spark.executor.memory");
    renameIfKeyExists(config, "EXECCORES", "spark.executor.cores");
    renameIfKeyExists(config, "NUM_GPUS", "spark.executor.gpus");

    renameIfKeyExists(config, "NUMEXECS", "spark.executor.instances");

    renameIfKeyExists(config, "DYNEXECS", "spark.dynamicAllocation.enabled");
    renameIfKeyExists(config, "DYNEXECSMINSELECTED", "spark.dynamicAllocation.minExecutors");
    renameIfKeyExists(config, "DYNEXECSMAXSELECTED", "spark.dynamicAllocation.maxExecutors");
    renameIfKeyExists(config, "DYNEXECSINIT", "spark.dynamicAllocation.initialExecutors");

    //If kafka config exists
    if(config.has("KAFKA")) {
      renameKafka(config);
    }

    if(config.has("SCHEDULE")) {
      renameSchedule(config);
    }

    return config.toString();
  }

  private void renameIfKeyExists(JSONObject config, String oldKey, String newKey) {
    if(config.has(oldKey)) {
      Object oldKeyValue = config.get(oldKey);
      config.remove(oldKey);
      config.put(newKey, oldKeyValue);
    }
  }

  private void removeKeyIfExists(JSONObject config, String key) {
    if(config.has(key)) {
      config.remove(key);
    }
  }

  private void addKeyValue(JSONObject config, String key, Object value) {
    config.put(key, value);
  }

  private void renameKafka(JSONObject config) {
    renameIfKeyExists(config, "KAFKA", "kafka");

    JSONObject kafkaObj =  (JSONObject)config.get("kafka");

    if(kafkaObj.has("TOPICS")) {
      renameIfKeyExists(kafkaObj, "TOPICS", "topics");
      JSONObject topicsObj = (JSONObject)kafkaObj.get("topics");
      Set<String> topics = topicsObj.keySet();
      for(String topic: topics) {
        JSONObject topicObj = (JSONObject)topicsObj.get(topic);
        renameIfKeyExists(topicObj, "TICKED", "ticked");
        renameIfKeyExists(topicObj, "NAME", "name");
      }
    }

    if(kafkaObj.has("CONSUMER_GROUPS")) {
      renameIfKeyExists(kafkaObj, "CONSUMER_GROUPS", "consumerGroups");
      JSONObject topicsObj = (JSONObject)kafkaObj.get("consumerGroups");
      Set<String> topics = topicsObj.keySet();
      for(String topic: topics) {
        JSONObject topicObj = (JSONObject)topicsObj.get(topic);
        renameIfKeyExists(topicObj, "ID", "id");
        renameIfKeyExists(topicObj, "NAME", "name");
      }
    }

    if(kafkaObj.has("ADVANCED")) {
      renameIfKeyExists(kafkaObj, "ADVANCED", "advanced");
    }
  }

  private void renameSchedule(JSONObject config) {
    renameIfKeyExists(config, "SCHEDULE", "schedule");

    JSONObject scheduleObj = (JSONObject)config.get("schedule");

    if(scheduleObj.has("NUMBER")) {
      renameIfKeyExists(scheduleObj, "NUMBER", "number");
    }

    if(scheduleObj.has("UNIT")) {
      renameIfKeyExists(scheduleObj, "UNIT", "unit");
    }

    if(scheduleObj.has("START")) {
      renameIfKeyExists(scheduleObj, "START", "start");
    }
  }

  @Override
  public void rollback() throws RollbackException {
    //migration is done inside a transaction so rollback is handled by the database
  }
}
