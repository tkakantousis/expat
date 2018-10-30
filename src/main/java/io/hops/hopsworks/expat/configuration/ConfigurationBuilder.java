package io.hops.hopsworks.expat.configuration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.ClasspathLocationStrategy;

public class ConfigurationBuilder {

  public static Configuration getConfiguration() throws ConfigurationException  {
    Parameters params = new Parameters();

    return new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
        .configure(params.xml()
            .setFileName("expat-site.xml")
            .setLocationStrategy(new ClasspathLocationStrategy())
            .setValidating(false))
        .getConfiguration();
  }
}
