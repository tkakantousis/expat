package io.hops.hopsworks.expat;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import static org.kohsuke.args4j.OptionHandlerFilter.ALL;

public class Expat {

  @Option(name="-a", usage="Action to take: migrate/rollback")
  private String command = "migrate";

  @Option(name="-v", usage="Version to migrate to or to rollback")
  private String version;

  public Expat(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);

    try {
      // parse the arguments.
      parser.parseArgument(args);

    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      System.err.println("java Expat [options...]");
      // print the list of available options
      parser.printUsage(System.err);
      System.err.println();

      // print option sample. This is useful some time
      System.err.println("  Example: java Expat"+parser.printExample(ALL));
      System.exit(1);
    }
  }

  public void run() throws ConfigurationException, MigrationException, RollbackException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    Configuration config = ConfigurationBuilder.getConfiguration();
    String migrations = config.getString("version-" + version.replace(".", ""));

    String[] migrationClasses = migrations.split("\n");
    for (String migration : migrationClasses) {
      MigrateStep step = (MigrateStep) Class.forName(migration.trim()).newInstance();
      if (command.equalsIgnoreCase("migrate")) {
        step.migrate();
      } else {
        step.rollback();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Expat e = new Expat(args);
    e.run();
  }
}
