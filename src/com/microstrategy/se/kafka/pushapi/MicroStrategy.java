/**
* This MicroStrategy class implements MicroStrategyLibrary API methods required
* to push data to a MicroStrategy Cube
*
* @author  Alex Fernandez
* @version 0.1
* @since   2018-08-01 
*
*/

package com.microstrategy.se.kafka.pushapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MicroStrategy {

	private String baseUrl;
	private String username;
	private String password;
	private CloseableHttpClient httpClient;
	private HttpClientContext httpContext;
	private Collection<Header> headers;
	private ObjectMapper mapper;
	private String projectId;
	private String datasetName;
	private String tableName;
	private String datasetId;

	// method for testing purposes only
	public static void main(String[] args) throws Exception {
		
		ArrayList<Header> headers1 = new ArrayList<Header>();
		headers1.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
		headers1.add(new BasicHeader(HttpHeaders.ACCEPT, "application/json"));
		HttpPost request = new HttpPost("/auth/login");
		request.setHeaders(headers1.toArray(new Header[headers1.size()]));
		
		for (Header header : request.getAllHeaders()) {
			System.out.println(header.getName()+":"+header.getValue());
		}
		
		System.exit(0);
		
		String libraryUrl = "https://xxx.microstrategy.com/MicroStrategyLibrary";
		String username = "usename";
		String password = "password";

		MicroStrategy mstr = new MicroStrategy(libraryUrl, username, password);
		mstr.connect();

		mstr.setProject("MicroStrategy Tutorial");
		mstr.setTarget("Streaming", "topic");
		Collection<SinkRecord> records = null;
		mstr.put(records);

	}

	private void put(Collection<SinkRecord> records) {
		for (SinkRecord record : records) {
			record.value();
		}
	}

	public MicroStrategy(String libraryUrl, String username, String password) {
		this.baseUrl = libraryUrl + "/api";
		this.username = username;
		this.password = password;

		CookieStore cookieStore = new BasicCookieStore();
		httpContext = HttpClientContext.create();
		httpContext.setCookieStore(cookieStore);
		httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();

		headers = new ArrayList<Header>();
		headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
		headers.add(new BasicHeader(HttpHeaders.ACCEPT, "application/json"));

		mapper = new ObjectMapper();
	}

	public void connect() throws MicroStrategyException {
		Map<String, String> payload = new Hashtable<String, String>();
		payload.put("username", username);
		payload.put("password", password);
		try {
			HttpPost request = new HttpPost(baseUrl + "/auth/login");
			request.setEntity(new StringEntity(mapper.writeValueAsString(payload)));
			request.setHeaders(headers.toArray(new Header[headers.size()]));
			CloseableHttpResponse response = httpClient.execute(request, httpContext);
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT) {
				headers.add(response.getFirstHeader("X-MSTR-AuthToken"));
			} else {
				throw new MicroStrategyException(response.getStatusLine().toString());
			}
		} catch (Exception e) {
			throw new MicroStrategyException(e);
		}
	}

	public void setProject(String projectName) throws MicroStrategyException {
		HttpGet request = new HttpGet(baseUrl + "/projects");
		request.setHeaders(headers.toArray(new Header[headers.size()]));
		try {
			CloseableHttpResponse response = httpClient.execute(request, httpContext);
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				List<Map<String, Object>> json = mapper.readValue(response.getEntity().getContent(),
						new TypeReference<ArrayList<Object>>() {
						});
				for (Map<String, Object> project : json) {
					if (projectName.equals(project.get("name"))) {
						projectId = (String) project.get("id");
						headers.add(new BasicHeader("X-MSTR-ProjectID", projectId));
						return;
					}
				}
				throw new MicroStrategyException("Project " + projectName + " was not found.");
			} else {
				throw new MicroStrategyException(response.getStatusLine().toString());
			}
		} catch (IOException e) {
			throw new MicroStrategyException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public void setTarget(String datasetName, String tableName) throws MicroStrategyException {
		this.datasetName = datasetName;
		this.tableName = tableName;

		try {
			URIBuilder uriBuilder = new URIBuilder(baseUrl + "/searches/results");
			uriBuilder.addParameter("limit", "-1");
			uriBuilder.addParameter("name", datasetName);
			uriBuilder.addParameter("type", "3"); // OBJECT_TYPE_REPORT_DEFINITION
			uriBuilder.addParameter("pattern", "2"); // SEARCH_TYPE_EXACTLY

			HttpGet request = new HttpGet(uriBuilder.build());
			request.setHeaders(headers.toArray(new Header[headers.size()]));
			CloseableHttpResponse response = httpClient.execute(request, httpContext);

			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				Map<String, Object> json = mapper.readValue(response.getEntity().getContent(),
						new TypeReference<Map<String, Object>>() {
						});
				if (((Integer) json.get("totalItems")) > 0) {
					datasetId = (String) ((List<Map<String, Object>>) json.get("result")).get(0).get("id");
				}
			} else {
				throw new MicroStrategyException(response.getStatusLine().toString());
			}
		} catch (Exception e) {
			throw new MicroStrategyException(e);
		}
	}

	public void push(Map<String, Object> tableDefinition) throws MicroStrategyException {
		try {
			Map<String, Object> payload = null;
			HttpEntityEnclosingRequestBase request = null;
			if (datasetId != null) { // Upsert
				request = new HttpPatch(baseUrl + "/datasets/" + datasetId + "/tables/" + tableName);
				request.setHeaders(headers.toArray(new Header[headers.size()]));
				request.setHeader("updatePolicy", "Upsert");
				payload = tableDefinition;
			} else { // Create dataset
				request = new HttpPost(baseUrl + "/datasets");
				request.setHeaders(headers.toArray(new Header[headers.size()]));
				payload = new HashMap<String, Object>();
				payload.put("name", datasetName);
				payload.put("tables", Collections.singletonList(tableDefinition));
			}
			// System.out.println(request.getURI());
			request.setEntity(new StringEntity(mapper.writeValueAsString(payload)));
			// System.out.println(mapper.writeValueAsString(payload));
			CloseableHttpResponse response = httpClient.execute(request, httpContext);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new MicroStrategyException(response.getStatusLine().toString());
			}
		} catch (IOException e) {
			throw new MicroStrategyException(e);
		}
	}

	private void printResponse(CloseableHttpResponse response) throws ParseException, IOException {
		HttpEntity entity = response.getEntity();
		System.out.println(EntityUtils.toString(entity, "utf-8"));
	}

}
