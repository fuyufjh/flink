/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.SessionDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;

/**
 * Entry point for the standalone session cluster.
 */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

	public StandaloneSessionClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new SessionDispatcherResourceManagerComponentFactory(StandaloneResourceManagerFactory.INSTANCE);
	}

	public static void main(String[] args) {
		// vmstat, iostat -> /home/hadoop/flink-1.9-tpcds-master/log/
		osMonitor("vmstat", "vmstat 1 2000");
		osMonitor("iostat", "iostat -xtc 1 2000");
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		EntrypointClusterConfiguration entrypointClusterConfiguration = null;
		final CommandLineParser<EntrypointClusterConfiguration> commandLineParser = new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

		try {
			entrypointClusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(StandaloneSessionClusterEntrypoint.class.getSimpleName());
			System.exit(1);
		}

		Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

		StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(entrypoint);
	}

	private static void osMonitor(String fileName, String cmd) {
		File logDir = new File("/disk/1/flink-1.9-tpcds-master/log/");
		if(logDir.exists() && logDir.isDirectory()) {
			File osLog = new File(logDir, fileName + ".log");
			try {
				osLog.createNewFile();
				// start vmstat log
				new Thread(()->{
					runProcess(osLog, cmd);
				}).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			LOG.info(logDir.getAbsolutePath() + " does not exist");
		}
	}

	private static void runProcess(File file, String cmd) {
		try {
			Process ps = Runtime.getRuntime().exec(cmd);

			BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream(), Charset.forName("GBK")));
			PrintStream out = new PrintStream(new FileOutputStream(file));
			String line = null;
			while ((line = br.readLine()) != null) {
				out.println(line);
			}

			br.close();
			ps.waitFor();
			LOG.info(cmd + " wait over ...");

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
