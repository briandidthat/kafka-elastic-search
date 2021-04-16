package com.organicautonomy.kafkaelasticsearch;

import com.google.gson.JsonParser;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static RestHighLevelClient createClient() {
        Dotenv env = Dotenv.load();

        final String hostname = env.get("HOSTNAME");
        final String username = env.get("USERNAME");
        final String password = env.get("PASSWORD");

        // create and configure credentials provider for HTTP requests
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // create rest client for performing HTTP requests
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServer = "localhost:9092";
        String groupId = "third_application";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20"); // limit the max polled records to 20

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }

    private static String extractIdFromTweet(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        // create rest high level client for api calls to elastic search
        RestHighLevelClient client = createClient();

        // create kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // loop through through the records in kafka and send to elastic search
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // log count of records
            logger.info("Received " + records.count());

            // create bulk request object for sending data to elastic search in batches
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // extract unique id from data to ensure idempotence using twitter feed
                    String id = extractIdFromTweet(record.value());

                    // extract value from record
                    String jsonString = record.value();

                    // create IndexRequest object to send data to elastic
                    IndexRequest indexRequest = new IndexRequest("twitter");
                    indexRequest.id(id); // set the id we extracted previously to ensure idempotence
                    indexRequest.source(jsonString, XContentType.JSON); // add data to request object

                    // add the request into a bulk request object to optimize performance
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: " + record.value());
                }
            }

            if (records.count() > 0) {
                // send data to elastic search and get response using BulkResponse object
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed.");
            }

        }

//         close the connection
//        client.close();
    }
}
