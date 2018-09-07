/**
* This class sends data from a Kafka topic to a MicroStrategy Cube using Push
* API calls
* 
* It automatically converts INTEGER and STRING values to Attributes
* DOUBLE values are converted to Metrics
* 
* Unhandled formats : BIGINTEGER, BOOL, BIGDECIMAL, DATE, TIME, DATETIME
*
* @author  Alex Fernandez
* @version 0.1
* @since   2018-08-01 
*
*/

package com.microstrategy.se.kafka.pushapi;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SinkImpl extends SinkTask {

	private Collection<Map<String, String>> columns = null;
	final private ObjectMapper mapper = new ObjectMapper();
	private long lastEvent = System.currentTimeMillis();
	static final private HashMap<Class<?>, String> mstrTypes = new HashMap<Class<?>, String>();
	// final private Map<Object, Map<String, Object>> buffer = new HashMap<Object,
	// Map<String, Object>>();
	final private List<Map<String, Object>> buffer = new ArrayList<Map<String, Object>>();

	private Map<String, String> props;

	static {
		mstrTypes.put(Long.class, "INTEGER"); // INTEGER and STRING converted to Attribute
		mstrTypes.put(Double.class, "DOUBLE"); // DOUBLE converted to Metric
	}

	@Override
	public String version() {
		return "0.0.1a";
	}

	@Override
	@SuppressWarnings("unchecked")
	public void put(Collection<SinkRecord> records) {
		for (SinkRecord record : records) {
			// buffer.put(record.key(), (Map<String, Object>) record.value());
			Map<String, Object> value = (Map<String, Object>) record.value();
			for (Entry<String, Object> entry : value.entrySet()) {
				if (entry.getValue().getClass().equals(Long.class)) {
					entry.setValue(entry.getValue().toString());
				}
			}
			buffer.add(value);
		}
		if (buffer.size() > 5000) {
			System.out.println("Requesting early commit due to data volume...");
			context.requestCommit();
		}
	}

	private void timeSinceLastEvent() {
		long currentTime = System.currentTimeMillis();
		System.out.println("Since last event " + (currentTime - lastEvent) + " miliseconds.");
		lastEvent = currentTime;
	}

	private Collection<Map<String, String>> getColumns(Map<String, Object> row) {
		Collection<Map<String, String>> columns = new ArrayList<Map<String, String>>();
		for (Entry<String, Object> entry : row.entrySet()) {
			Map<String, String> entryMap = new HashMap<String, String>();
			entryMap.put("name", entry.getKey());
			entryMap.put("dataType", mstrTypes.getOrDefault(entry.getValue().getClass(), "STRING"));
			columns.add(entryMap);
		}
		return columns;
	}

	@Override
	public void start(Map<String, String> props) {
		System.out.println("SinkImpl.start()");
		this.props = props;
		for (Entry<String, String> entry : props.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

	@Override
	public void stop() {
		System.out.println("SinkImpl.stop()");
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		System.out.println("*** SinkImpl.flush() ***");
		System.out.println("Flushing " + buffer.size() + " records.");
		timeSinceLastEvent();
		String tableName = "topic";
		if (!buffer.isEmpty()) {
			try {
				Map<String, Object> tableDefinition = new HashMap<String, Object>();
				tableDefinition.put("name", tableName);
				if (columns == null) {
					columns = getColumns(buffer.iterator().next());
				}
				tableDefinition.put("columnHeaders", columns);

				byte[] array = mapper.writeValueAsString(buffer).getBytes(Charset.forName("utf-8"));
				tableDefinition.put("data", new String(Base64.getEncoder().encode(array), Charset.forName("utf-8")));

				MicroStrategy mstr = new MicroStrategy(props.get(MicroStrategySink.CONFIG_LIBRARY_URL),
						props.get(MicroStrategySink.CONFIG_USER), props.get(MicroStrategySink.CONFIG_PASSWORD));
				mstr.connect();
				mstr.setProject(props.get(MicroStrategySink.CONFIG_PROJECT));
				mstr.setTarget(props.get(MicroStrategySink.CONFIG_CUBE), tableName);
				mstr.push(tableDefinition);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				System.err.println(buffer.size() + " records failed to load.");
			} catch (MicroStrategyException e) {
				e.printStackTrace();
				System.err.println(buffer.size() + " records failed to load.");
			}
			buffer.clear();
		}
		super.flush(currentOffsets);
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		System.out.println("SinkImpl.preCommit()");
		return super.preCommit(currentOffsets);
	}

}
