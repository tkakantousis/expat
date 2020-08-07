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
package io.hops.hopsworks.expat.migrations.dataset;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.FsPermissions;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDataset;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetFacade;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetSharedWith;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetSharedWithFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInodeFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsGroup;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsGroupFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUser;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUserFacade;
import io.hops.hopsworks.expat.db.dao.project.ExpatProject;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectFacade;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectMember;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectMemberFacade;
import io.hops.hopsworks.expat.migrations.projects.provenance.HopsClient;
import io.hops.hopsworks.persistence.entity.dataset.DatasetAccessPermission;
import io.hops.hopsworks.persistence.entity.project.team.ProjectRoleTypes;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FixDatasetPermissionHelper {
  private static final Logger LOGGER = LogManager.getLogger(FixDatasetPermission.class);
  
  private Connection connection;
  private ExpatProjectFacade projectFacade;
  private ExpatDatasetFacade datasetFacade;
  private ExpatDatasetSharedWithFacade datasetSharedWithFacade;
  private ExpatProjectMemberFacade projectMemberFacade;
  private ExpatHdfsGroupFacade hdfsGroupFacade;
  private ExpatHdfsUserFacade hdfsUserFacade;
  private ExpatHdfsInodeFacade inodeFacade;
  private String hopsUser;
  
  public FixDatasetPermissionHelper() {
  }
  
  public void setup() throws SQLException, ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    this.hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    this.connection = DbConnectionFactory.getConnection();
    this.projectFacade = new ExpatProjectFacade(ExpatProject.class, this.connection);
    this.datasetFacade = new ExpatDatasetFacade(ExpatDataset.class, this.connection);
    this.datasetSharedWithFacade = new ExpatDatasetSharedWithFacade(ExpatDatasetSharedWith.class, this.connection);
    this.projectMemberFacade = new ExpatProjectMemberFacade(ExpatProjectMember.class, this.connection);
    this.hdfsGroupFacade = new ExpatHdfsGroupFacade(ExpatHdfsGroup.class, this.connection);
    this.hdfsUserFacade = new ExpatHdfsUserFacade(ExpatHdfsUser.class, this.connection);
    this.inodeFacade = new ExpatHdfsInodeFacade(ExpatHdfsInode.class, this.connection);
  }
  
  private void fixPermission() throws IllegalAccessException, SQLException, InstantiationException, IOException {
    List<ExpatProject> projects = this.projectFacade.findAll();
    for (int i = 0; i < projects.size(); i++) {
      fixPermission(projects.get(i));
    }
  }
  
  private void fixPermission(ExpatProject expatProject) throws IllegalAccessException, SQLException,
    InstantiationException, IOException {
    if (isUnderRemoval(expatProject)) {
      return;
    }
    List<ExpatDataset> datasetList = this.datasetFacade.findByProjectId(expatProject.getId());
    for (ExpatDataset dataset : datasetList) {
      fixDataset(dataset, expatProject);
    }
  }
  
  private void fixDataset(ExpatDataset dataset, ExpatProject expatProject) throws IllegalAccessException, SQLException,
    InstantiationException, IOException {
    DistributedFileSystemOps dfso = null;
    try {
      dfso = HopsClient.getDFSO(this.hopsUser);
      fixPermission(expatProject, dataset, dfso);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
    }
  }
  
  private void fixPermission(ExpatProject expatProject, ExpatDataset dataset, DistributedFileSystemOps dfso)
    throws IllegalAccessException, SQLException, InstantiationException, IOException {
    String datasetGroup = getHdfsGroupName(expatProject.getName(), dataset);
    String datasetAclGroup = getHdfsAclGroupName(expatProject.getName(), dataset);
    ExpatHdfsGroup hdfsDatasetGroup = getOrCreateGroup(datasetGroup, dfso);
    ExpatHdfsGroup hdfsDatasetAclGroup = getOrCreateGroup(datasetAclGroup, dfso);
    if (hdfsDatasetGroup == null) {
      throw new IllegalStateException("Failed to get group=" + datasetGroup);
    }
    if (hdfsDatasetAclGroup == null) {
      throw new IllegalStateException("Failed to get group=" + datasetAclGroup);
    }
    if (dataset.getPublicDs() > 0 && !DatasetAccessPermission.READ_ONLY.getValue().equals(dataset.getPermission())) {
      datasetFacade.updatePermission(dataset.getId(), DatasetAccessPermission.READ_ONLY.getValue());
      dataset = datasetFacade.find(dataset.getId());
    }
    ExpatHdfsInode inode = this.inodeFacade.find(dataset.getInodeId());
    ExpatHdfsUser hdfsUser = this.hdfsUserFacade.find(inode.getHdfsUser());
    List<ExpatProjectMember> datasetTeamCollection = new ArrayList<>();
    List<ExpatProjectMember> projectMembers = this.projectMemberFacade.findByProjectId(expatProject.getId());
    datasetTeamCollection.addAll(projectMembers);
    testFsPermission(dataset, inode, dfso);
    testAndFixPermissionForAllMembers(projectMembers, dfso, hdfsDatasetGroup, hdfsDatasetAclGroup,
      hdfsUser, dataset.getPermission());
    List<ExpatDatasetSharedWith> datasetSharedWithList = this.datasetSharedWithFacade.findByDatasetId(dataset.getId());
    for (ExpatDatasetSharedWith datasetSharedWith : datasetSharedWithList) {
      if (dataset.getPublicDs() > 0 && !DatasetAccessPermission.READ_ONLY.getValue().equals(datasetSharedWith.getPermission())) {
        datasetSharedWithFacade
          .updatePermission(datasetSharedWith.getId(), DatasetAccessPermission.READ_ONLY.getValue());
        datasetSharedWith = datasetSharedWithFacade.find(datasetSharedWith.getId());
      }
      projectMembers = this.projectMemberFacade.findByProjectId(datasetSharedWith.getProject());
      datasetTeamCollection.addAll(projectMembers);
      testAndFixPermissionForAllMembers(projectMembers, dfso, hdfsDatasetGroup, hdfsDatasetAclGroup,
        null, datasetSharedWith.getPermission());
    }
    testAndRemoveUsersFromGroup(datasetTeamCollection, hdfsDatasetGroup, hdfsDatasetAclGroup,
      hdfsUser, dfso);
  }
  
  private void testAndRemoveUsersFromGroup(List<ExpatProjectMember> datasetTeamCollection,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsGroup hdfsDatasetAclGroup, ExpatHdfsUser hdfsUser,
    DistributedFileSystemOps dfso) {
    
  }
  
  private void testAndFixPermissionForAllMembers(List<ExpatProjectMember> projectMembers, DistributedFileSystemOps dfso,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsGroup hdfsDatasetAclGroup, ExpatHdfsUser hdfsUser, String permission) {
    
  }
  
  private void testFsPermission(ExpatDataset dataset, ExpatHdfsInode inode, DistributedFileSystemOps dfso)
    throws IllegalAccessException, SQLException, InstantiationException {
    FsPermission fsPermission = FsPermission.createImmutable(inode.getPermission());
    FsPermission fsPermissionReadOnly = FsPermission.createImmutable((short) 00550);
    FsPermission fsPermissionReadOnlyT = FsPermission.createImmutable((short) 01550);
    FsPermission fsPermissionDefault = FsPermissions.rwxrwx___;
    FsPermission fsPermissionDefaultT = FsPermissions.rwxrwx___T;
    Path path = new Path(getPath(inode));
    if (dataset.getPublicDs() > 0 && !fsPermissionReadOnly.equals(fsPermission) &&
      !fsPermissionReadOnlyT.equals(fsPermission)) {
      makeImmutable(path, dfso);
    }
    if (dataset.getPublicDs() == 0 && !fsPermissionDefault.equals(fsPermission) &&
      !fsPermissionDefaultT.equals(fsPermission)) {
      undoImmutable(path, dfso);
    }
  }
  
  private void undoImmutable(Path path, DistributedFileSystemOps dfso) {
  
  }
  
  private void makeImmutable(Path path, DistributedFileSystemOps dfso) {
  
  }
  
  private String getPath(ExpatHdfsInode inode) throws IllegalAccessException, SQLException, InstantiationException {
    StringBuilder path = new StringBuilder(inode.getName());
    ExpatHdfsInode parentInode = this.inodeFacade.find(inode.getParentId());
    while (parentInode != null) {
      path.insert(0, parentInode.getName() + File.separator);
      parentInode = this.inodeFacade.find(parentInode.getParentId());
    }
    return path.toString();
  }
  
  private ExpatHdfsGroup getOrCreateGroup(String group, DistributedFileSystemOps dfso) throws IllegalAccessException,
    SQLException, InstantiationException, IOException {
    ExpatHdfsGroup hdfsGroup = this.hdfsGroupFacade.findByName(group);
    if (hdfsGroup == null) {
      dfso.addGroup(group);
      hdfsGroup = this.hdfsGroupFacade.findByName(group);
      LOGGER.info("Found and fixed a missing group: group=" + group);
    }
    return hdfsGroup;
  }
  
  private String getHdfsAclGroupName(String projectName, ExpatDataset dataset) {
    return getHdfsGroupName(projectName, dataset) + HdfsUsersController.USER_NAME_DELIMITER + "read";
  }
  
  private String getHdfsGroupName(String projectName, ExpatDataset dataset) {
    return projectName + HdfsUsersController.USER_NAME_DELIMITER + dataset.getName();
  }
  
  private boolean isUnderRemoval(ExpatProject expatProject) throws IllegalAccessException, SQLException,
    InstantiationException {
    List<ExpatProjectMember> members = this.projectMemberFacade.findByProjectId(expatProject.getId());
    for (ExpatProjectMember member : members) {
      if (ProjectRoleTypes.UNDER_REMOVAL.equals(member.getTeamRole())) {
        return true;
      }
    }
    return false;
  }
}
