package org.zenframework.camel.runner;

import groovy.lang.GroovyShell;
import groovyjarjarcommonscli.BasicParser;
import groovyjarjarcommonscli.CommandLine;
import groovyjarjarcommonscli.Options;
import groovyjarjarcommonscli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.StatefulService;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spring.SpringCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class CamelRunner {

	private static final Logger LOG = LoggerFactory.getLogger(CamelRunner.class);

	private static final String CAMEL_CONTEXT_URI_DEFAULT = "classpath:camel-context.xml";
	private static final String ROUTES_PATH_DEFAULT = "../routes";
	private static final String GROOVY_SUFFIX = ".groovy";

	// Groovy DSL folder
	private String routesPath;
	// Camel context for extensions
	private String camelContextUri;

	private CamelContext context;

	public CamelRunner() {
		this(CAMEL_CONTEXT_URI_DEFAULT, ROUTES_PATH_DEFAULT);
	}

	public CamelRunner(String camelContextUri, String routesPath) {
		this.routesPath = routesPath;
		this.camelContextUri = camelContextUri;
	}

	public void start() throws Exception {

		context = getCamelContext();
		GroovyShell shell = new GroovyShell();

		for (File file : new File(routesPath).listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.getName().endsWith(GROOVY_SUFFIX);
			}

		})) {
			try {
				context.addRoutes((RouteBuilder) shell.evaluate(file));
			} catch (Exception e) {
				LOG.error("Can't add route from '" + file + "'", e);
			}
		}

		context.start();
		LOG.info("Camel context started");

	}

	public void stop() throws Exception {
		context.stop();
		try {
			StatefulService service = (StatefulService) context;
			while (!service.isStopped()) {
				Thread.sleep(100);
			}
		} catch (Throwable e) {
			LOG.error("Can't wait for Camel context stopped", e);
		}
		for (Route route : context.getRoutes()) {
			context.removeRoute(route.getId());
		}
		LOG.info("Camel context stopped");
	}

	public String getRoutesPath() {
		return routesPath;
	}

	public void setRoutesPath(String routesPath) {
		this.routesPath = routesPath;
	}

	public String getCamelContextUri() {
		return camelContextUri;
	}

	public void setCamelContextUri(String camelContextUri) {
		this.camelContextUri = camelContextUri;
	}

	private CamelContext getCamelContext() {
		ApplicationContext appContext;
		if (camelContextUri.startsWith("file:"))
			appContext = new FileSystemXmlApplicationContext(camelContextUri);
		else if (camelContextUri.startsWith("classpath:"))
			appContext = new ClassPathXmlApplicationContext(camelContextUri);
		else
			throw new Error("Unsupported URI protocol: " + camelContextUri);
		return new SpringCamelContext(appContext);
	}

	public static void main(String[] args) throws ParseException {

		final AtomicBoolean active = new AtomicBoolean(true);
		final CamelRunner camelRunner = new CamelRunner();

		CommandLine cmd = new BasicParser().parse(getOptions(), args);
		if (cmd.hasOption('c'))
			camelRunner.setCamelContextUri(cmd.getOptionValue('c'));
		if (cmd.hasOption('r'))
			camelRunner.setRoutesPath(cmd.getOptionValue('r'));

		try {

			camelRunner.start();

			Runtime.getRuntime().addShutdownHook(new Thread("Hook") {

				@Override
				public void run() {
					LOG.info("Shutdown hook invoked");
					try {
						camelRunner.stop();
					} catch (Exception e) {
						LOG.error("Can't stop Apache Camel", e);
					}
					active.set(false);
				}

			});

			Thread console = new Thread("Console") {

				@Override
				public void run() {
					BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
					try {
						while (active.get()) {
							String line = in.readLine().trim().toLowerCase();
							if (Arrays.asList("exit", "stop", "quit").contains(line)) {
								try {
									camelRunner.stop();
								} catch (Exception e) {
									LOG.error("Can't stop Apache Camel", e);
								}
								active.set(false);
							} else if (Arrays.asList("reload", "restart").contains(line)) {
								try {
									camelRunner.stop();
									camelRunner.start();
								} catch (Exception e) {
									LOG.error("Can't restart Apache Camel", e);
									active.set(false);
								}
							} else if (Arrays.asList("?", "help").contains(line)) {
								System.out.println("\nUsage:\nexit, stop, quit - stop program");
							}
						}
					} catch (IOException e) {
						LOG.warn("Can't read from stdin", e);
					} finally {
						LOG.info("Console closed");
					}
				}

			};
			console.setDaemon(true);
			console.start();

			while (active.get()) {
				Thread.sleep(500);
			}

		} catch (Exception e) {
			LOG.error("Can't start Apache Camel", e);
		}

	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption("c", "camelContextUri", true, "Camel context URI (file: or classpath:");
		options.addOption("r", "routesPath", true, "Routes folder path");
		return options;
	}

}
