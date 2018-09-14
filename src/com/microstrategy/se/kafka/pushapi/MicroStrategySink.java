/**
* Configuration class for the MicroStrategySink Kafka connector
*
* @author  Alex Fernandez
* @version 0.1
* @since   2018-08-01 
*
*/

package com.microstrategy.se.kafka.pushapi;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class MicroStrategySink extends SinkConnector {

	static final String CONFIG_LIBRARY_URL = "CONFIG_LIBRARY_URL";
	static final String CONFIG_USER = "CONFIG_USER";
	static final String CONFIG_PASSWORD = "CONFIG_PASSWORD";
	static final String CONFIG_PROJECT = "CONFIG_PROJECT";
	static final String CONFIG_CUBE = "CONFIG_CUBE";
	static final String CONFIG_FOLDER = "CONFIG_FOLDER";
	private Map<String, String> props;

	@Override
	public ConfigDef config() {
		System.out.println("SinkConnectorImpl.config()");
		ConfigDef config = new ConfigDef();
		int groupOrder = 0;
		config.define(
				CONFIG_LIBRARY_URL,
				Type.STRING,
				null,
				new NonEmptyString(),
				Importance.HIGH,
				"Library URL.  http://<SERVER>/MicroStrategyLibrary",
				"MicroStrategy",
				++groupOrder,
				Width.LONG,
				"Library URL"
				);
		config.define(
				CONFIG_USER,
				Type.STRING,
				null,
				new NonEmptyString(),
				Importance.HIGH,
				"User name.",
				"MicroStrategy",
				++groupOrder,
				Width.SHORT,
				"User name");
		config.define(
				CONFIG_PASSWORD,
				Type.STRING,
				null,
				new NonEmptyString(),
				Importance.HIGH,
				"Password.",
				"MicroStrategy",
				++groupOrder,
				Width.SHORT,
				"User password");
		config.define(
				CONFIG_PROJECT,
				Type.STRING,
				null,
				new NonEmptyString(),
				Importance.HIGH,
				"Project name.",
				"MicroStrategy",
				++groupOrder,
				Width.MEDIUM,
				"Project name");
//		config.define(
//				CONFIG_FOLDER,
//				Type.STRING,
//				"D3C7D461F69C4610AA6BAA5EF51F4125",
//				new NonEmptyString(),
//				Importance.HIGH,
//				"Folder ID",
//				"MicroStrategy",
//				++groupOrder,
//				Width.MEDIUM,
//				"Target folderId");
		config.define(
				CONFIG_CUBE,
				Type.STRING,
				null,
				new NonEmptyString(),
				Importance.HIGH,
				"Target cube name.",
				"MicroStrategy",
				++groupOrder,
				Width.MEDIUM,
				"Target cube name");
		return config;
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
		System.out.println("SinkConnectorImpl.start()");
	}

	@Override
	public void stop() {
		System.out.println("SinkConnectorImpl.stop()");
	}

	@Override
	public Class<? extends Task> taskClass() {
		System.out.println("SinkConnectorImpl.taskClass()");
		return SinkImpl.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int num) {
		System.out.println("SinkConnectorImpl.taskConfigs()");
		return Collections.singletonList(props);
	}

	@Override
	public String version() {
		return "0.0.1a";
	}

}
