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

public class ExpatProject extends ExpatAbstractEntity<ExpatProject> {
  private Integer id;
  private String name;
  private String owner;
  private Date created;
  private Date retentionPeriod;
  private Boolean deleted;
  private String paymentType;
  private String pythonVersion;
  private String description;
  private Integer kafkaMaxNumTopics;
  private Date lastQuotaUpdate;
  private String dockerImage;
  private Long inodePId;
  private String inodeName;
  private Long partitionId;
  private Boolean conda = false;
  private Boolean archived = false;
  private Boolean logs = false;
  
  public ExpatProject() {
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getOwner() {
    return owner;
  }
  
  public void setOwner(String owner) {
    this.owner = owner;
  }
  
  public Date getCreated() {
    return created;
  }
  
  public void setCreated(Date created) {
    this.created = created;
  }
  
  public Date getRetentionPeriod() {
    return retentionPeriod;
  }
  
  public void setRetentionPeriod(Date retentionPeriod) {
    this.retentionPeriod = retentionPeriod;
  }
  
  public Boolean getDeleted() {
    return deleted;
  }
  
  public void setDeleted(Boolean deleted) {
    this.deleted = deleted;
  }
  
  public String getPaymentType() {
    return paymentType;
  }
  
  public void setPaymentType(String paymentType) {
    this.paymentType = paymentType;
  }
  
  public String getPythonVersion() {
    return pythonVersion;
  }
  
  public void setPythonVersion(String pythonVersion) {
    this.pythonVersion = pythonVersion;
  }
  
  public String getDescription() {
    return description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public Integer getKafkaMaxNumTopics() {
    return kafkaMaxNumTopics;
  }
  
  public void setKafkaMaxNumTopics(Integer kafkaMaxNumTopics) {
    this.kafkaMaxNumTopics = kafkaMaxNumTopics;
  }
  
  public Date getLastQuotaUpdate() {
    return lastQuotaUpdate;
  }
  
  public void setLastQuotaUpdate(Date lastQuotaUpdate) {
    this.lastQuotaUpdate = lastQuotaUpdate;
  }
  
  public String getDockerImage() {
    return dockerImage;
  }
  
  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }
  
  public Long getInodePId() {
    return inodePId;
  }
  
  public void setInodePId(Long inodePId) {
    this.inodePId = inodePId;
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public Long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }
  
  public Boolean getConda() {
    return conda;
  }
  
  public void setConda(Boolean conda) {
    this.conda = conda;
  }
  
  public Boolean getArchived() {
    return archived;
  }
  
  public void setArchived(Boolean archived) {
    this.archived = archived;
  }
  
  public Boolean getLogs() {
    return logs;
  }
  
  public void setLogs(Boolean logs) {
    this.logs = logs;
  }
  
  @Override
  public ExpatProject getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.name = resultSet.getString("projectname");
    this.owner = resultSet.getString("email");
    this.created = resultSet.getDate("created");
    this.retentionPeriod = resultSet.getDate("retention_period");
    this.deleted = resultSet.getBoolean("deleted");
    this.paymentType = resultSet.getString("payment_type");
    this.pythonVersion = resultSet.getString("python_version");
    this.description = resultSet.getString("description");
    this.kafkaMaxNumTopics = resultSet.getInt("kafka_max_num_topics");
    this.lastQuotaUpdate = resultSet.getDate("last_quota_update");
    this.dockerImage = resultSet.getString("docker_image");
    this.inodePId = resultSet.getLong("inode_pid");
    this.inodeName = resultSet.getString("inode_name");
    this.partitionId = resultSet.getLong("partition_id");
    this.conda = resultSet.getBoolean("conda");
    this.archived = resultSet.getBoolean("archived");
    this.logs = resultSet.getBoolean("logs");
    return this;
  }
}
