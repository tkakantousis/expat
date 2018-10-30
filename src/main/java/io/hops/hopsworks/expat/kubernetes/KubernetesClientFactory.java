package io.hops.hopsworks.expat.kubernetes;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class KubernetesClientFactory {

  private static Config kubeConfig = null;

  public static KubernetesClient getClient() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();

    if (kubeConfig == null) {
      kubeConfig = new ConfigBuilder()
          .withUsername(config.getString(ExpatConf.KUBE_USER_KEY))
          .withMasterUrl(config.getString(ExpatConf.KUBE_MASTER_URL_KEY))
          .withCaCertFile(config.getString(ExpatConf.KUBE_CA_CERTFILE_KEY))
          .withTrustStoreFile(config.getString(ExpatConf.KUBE_TSTORE_PATH_KEY))
          .withTrustStorePassphrase(config.getString(ExpatConf.KUBE_TSTORE_PWD_KEY))
          .withKeyStoreFile(config.getString(ExpatConf.KUBE_KSTORE_PATH_KEY))
          .withKeyStorePassphrase(config.getString(ExpatConf.KUBE_KSTORE_PWD_KEY))
          .withClientCertFile(config.getString(ExpatConf.KUBE_CERTFILE_KEY))
          .withClientKeyFile(config.getString(ExpatConf.KUBE_KEYFILE_KEY))
          .withClientKeyPassphrase(config.getString(ExpatConf.KUBE_KEYPWD_KEY))
          .build();
    }

    return new DefaultKubernetesClient(kubeConfig);
  }
}
