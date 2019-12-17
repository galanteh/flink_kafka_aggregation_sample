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

package com.galanteh.samples;

import java.util.Objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Sample on how to read from Kafka a JSON with a format like this
 *
 * {
 *    "timestamp":"1992-09-11 22:11:43",
 *    "user_agent":"Opera/8.69.(X11; Linux x86_64; my-MM) Presto/2.9.177 Version/12.00",
 *    "ip":"169.197.157.15",
 *    "email":"josefina65@hotmail.com",
 *    "first_name":"Ivan",
 *    "last_name":"Becerra",
 *    "country":"Azerbaiyán"
 * }
 *
 */

public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	public static class WebTraffic {

		private String timestamp;
		private String user_agent;
		private String ip;
		private String country;
		private String email;
		private String first_name;
		private String last_name;

		public String getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}

		public String getUser_agent() {
			return user_agent;
		}

		public void setUser_agent(String user_agent) {
			this.user_agent = user_agent;
		}

		public String getIp() {
			return ip;
		}

		public void setIp(String ip) {
			this.ip = ip;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getFirst_name() {
			return first_name;
		}

		public void setFirst_name(String first_name) {
			this.first_name = first_name;
		}

		public String getLast_name() {
			return last_name;
		}

		public void setLast_name(String last_name) {
			this.last_name = last_name;
		}

	}

	public static class WebTrafficAggregatorByCountry
			implements AggregateFunction<WebTraffic, Tuple2<String, Long>, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> createAccumulator() {
			return new Tuple2<String, Long>("", 0L);
		}

		@Override
		public Tuple2<String, Long> add(WebTraffic webTraffic, Tuple2<String, Long> accumulator) {
			accumulator.f0 = webTraffic.getCountry();
			accumulator.f1 += 1;
			return accumulator;
		}

		@Override
		public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
			return new Tuple2<String, Long>(a.f0, a.f1 + b.f1);
		}

	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		ParameterTool parameter = ParameterTool.fromArgs(args);
		if (parameter.getNumberOfParameters() < 5)
		{
			System.out.println("ERROR - Missing parameters!");
			System.out.println("Usage: webtraffic-by-country --topic <topic> --bootstrap.servers <kafka brokers host:port,host:port ...> --group.id <name of the group> --timeWindow <number of seconds>");
			return;
		}

		String bootstrap_servers = parameter.getRequired("bootstrap.servers");
		String zookeeper_servers = parameter.getRequired("zookeeper.servers");
		String topic = parameter.getRequired("topic");
		String group_id = parameter.get("group.id", "webtraffic-by-country");
		long timeWindow = parameter.getLong("timeWindow");
		LOG.info(" ******** PARAMETERS ******** ");
		LOG.info("Kafka Brokers: {}", bootstrap_servers);
		LOG.info("Kafka Zookeepers: {}", zookeeper_servers);
		LOG.info("Kafka Topic: {}", topic);
		LOG.info("Kafka Group: {}", group_id);
		LOG.info("Time windows to Process: {}", timeWindow);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrap_servers);
		properties.setProperty("zookeeper.connect", zookeeper_servers); // Zookeeper default host:port
		properties.setProperty("topic", topic);
		properties.setProperty("group.id", group_id);
		properties.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

		LOG.info("Starting Kafka Connector ...");
		FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> stream = env.addSource(kafkaSource);
		LOG.info("Stream created from Kafka, {}", stream);

		KeyedStream<WebTraffic, String> webTrafficPerCountryStream = stream.map(data -> {
			try
			{
				ObjectMapper mapper = new ObjectMapper();
				JsonNode jsonNode = mapper.readValue(data, JsonNode.class);
				String country = jsonNode.get("country").asText();
				WebTraffic webTraffic = new WebTraffic();
				webTraffic.setCountry(country);
				return webTraffic;
			}
			catch (Exception e)
			{
				LOG.info("ERROR while deserializing JSON data: " + data);
				LOG.info("ERROR: {}", e);
				return null;
			}
		}).filter(Objects::nonNull).keyBy(WebTraffic::getCountry);

		DataStream<Tuple2<String, Long>> result = webTrafficPerCountryStream
				.timeWindow(Time.seconds(15))
				.aggregate(new WebTrafficAggregatorByCountry());

		LOG.info("Results for the time window: {} ", result);
		result.print();

		// execute program
		LOG.info("Job WebTraffic by Country is starting ... ");
		env.execute("WebTraffic by Country");
	}
}


