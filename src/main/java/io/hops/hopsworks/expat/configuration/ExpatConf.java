package io.hops.hopsworks.expat.configuration;

public class ExpatConf {
  // ------ Database Configuration ------ //
  private static final String DATABASE_PREFIX = "database.";
  public static final String DATABASE_DBMS_DRIVER_NAME = DATABASE_PREFIX + "driver";
  public static final String DATABASE_DBMS_DRIVER_NAME_DEFAULT = "com.mysql.jdbc.Driver";

  public static final String DATABASE_URL = DATABASE_PREFIX + "url";
  public static final String DATABASE_USER_KEY = DATABASE_PREFIX + "user";
  public static final String DATABASE_PASSWORD_KEY = DATABASE_PREFIX + "password";

  // ------ Kubernetes Configuration ------ //
  private static final String KUBE_PREFIX = "kube.";
  public static final String KUBE_USER_KEY = KUBE_PREFIX + "user";
  public static final String KUBE_MASTER_URL_KEY = KUBE_PREFIX + "masterUrl";
  public static final String KUBE_CA_CERTFILE_KEY = KUBE_PREFIX + "caPath";
  public static final String KUBE_TSTORE_PATH_KEY = KUBE_PREFIX + "tstorePath";
  public static final String KUBE_TSTORE_PWD_KEY= KUBE_PREFIX + "tstorePwd";
  public static final String KUBE_KSTORE_PATH_KEY = KUBE_PREFIX + "kstorePath";
  public static final String KUBE_KSTORE_PWD_KEY = KUBE_PREFIX + "tstorePwd";

  public static final String KUBE_CERTFILE_KEY = KUBE_PREFIX + "certFile";
  public static final String KUBE_KEYFILE_KEY = KUBE_PREFIX + "keyFile";
  public static final String KUBE_KEYPWD_KEY = KUBE_PREFIX + "keyPwd";

  // ------- Other configuration -------- //
  public static final String MASTER_PWD_FILE_KEY = "masterPwdFile";
}
