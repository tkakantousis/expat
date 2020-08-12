#!/bin/bash
set -e

if [ $# -ne 6 ] ; then
    echo "Usage: conda_env_migrate.sh project conda_dir conda_user project_user hdfs_superuser hadoop_home"
    exit 1
fi

PROJECT=$1
CONDA_DIR=$2
CONDA_USER=$3
PROJECT_USER=$4
HDFS_SUPERUSER=$5
HADOOP_HOME=$6
YML_FILE=python_env_${PROJECT}.yml
YML_FILE_PATH=/tmp/${YML_FILE}

if [ -d "$CONDA_DIR/envs/$PROJECT" ]; then
  echo "exporting from $CONDA_DIR/envs/$PROJECT"
  # Export conda env for project
  su "$CONDA_USER" -c "${CONDA_DIR}/bin/conda env export -n ${PROJECT} > ${YML_FILE_PATH}"
  if [ $? -ne 0 ] ; then
      exit 1
  fi

  # Copy env yaml into Resources dataset and chown to project_user
  su "$HDFS_SUPERUSER" -c "${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal -f ${YML_FILE_PATH} /Projects/${PROJECT}/Resources/${YML_FILE} && ${HADOOP_HOME}/bin/hdfs dfs -chown ${PROJECT_USER}:${PROJECT_USER} /Projects/${PROJECT}/Resources/${YML_FILE}"
  if [ $? -ne 0 ] ; then
      # Remove exported env
      rm ${YML_FILE_PATH}
      exit 1
  fi
  exit 0
else
  exit 2
fi