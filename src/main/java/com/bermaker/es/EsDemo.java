package com.bermaker.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class EsDemo {

  private static Logger LOG = LoggerFactory.getLogger(EsDemo.class);

  public static void main(String[] args) {
    String storeFilePath = System.getenv("STORE_FILE_PATH");
    String storePassword = System.getenv("STORE_PASSWORD");

    Util.setSSLTrustStore(storeFilePath, storePassword);

    String serverUrl = "https://localhost:9200";
    String apiKey = "bW5Nc2VvNEJfeXA2NV9OdWp0dVU6M1NhVU9CMVVRb2VKNG1CT3kybnZIQQ==";

    // Create the low-level client
    RestClient restClient =
        RestClient.builder(HttpHost.create(serverUrl))
            .setDefaultHeaders(new Header[] {new BasicHeader("Authorization", "ApiKey " + apiKey)})
            .build();

    // Create the transport with a Jackson mapper
    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());

    // And create the API client
    // Synchronous blocking client
    ElasticsearchClient esClient = new ElasticsearchClient(transport);

    // Asynchronous non-blocking client
    ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);
    asyncClient
        .exists(b -> b.index("products").id("bk-1"))
        .whenComplete(
            (response, exception) -> {
              if (exception != null) {
                LOG.error("Failed to index", exception);
              } else {
                LOG.info("Response is {}", response.value());
              }
            });

    try {
      if (esClient.exists(b -> b.index("products").id("bk-1")).value()) {
        LOG.info("Product exists.");
        esClient.indices().delete(d -> d.index("products"));
      } else {
        LOG.info("Product does not exist.");
      }

      testCreateAndSearch(esClient);

      testIngestRowData(esClient);



    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      try {
        restClient.close();
        LOG.info("Closed rest client.");
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  private static void testIngestRowData(ElasticsearchClient esClient) throws IOException {
    // Using raw JSON data
    Reader input =
        new StringReader(
            "{'@timestamp': '2022-04-08T13:55:32Z', 'level': 'warn', 'message': 'Some log message'}"
                .replace('\'', '"'));

    IndexRequest<JsonData> dataIndexRequest = IndexRequest.of(i -> i.index("logs").withJson(input));
    IndexResponse response = esClient.index(dataIndexRequest);
    LOG.info("Indexed with version " + response.version());

  }

  private static void testCreateAndSearch(ElasticsearchClient esClient) throws IOException {
    // 1. Create an index
    CreateIndexResponse createIndexResponse = esClient.indices().create(c -> c.index("products"));
    LOG.info("Response of creating an index is {}", createIndexResponse);

    // 2. Ingest data and index a document
    Product product = new Product("bk-1", "City bike", 123.0);
    IndexResponse indexResponse =
        esClient.index(i -> i.index("products").id(product.getSku()).document(product));
    LOG.info("Indexed with version {}", indexResponse.version());

    IndexRequest<Product> request =
        IndexRequest.of(i -> i.index("products").id(product.getSku()).document(product));
    indexResponse = esClient.index(request);
    LOG.info("Indexed with version {}", indexResponse.version());

    // 3. Get documents
    GetResponse<Product> getResponse =
        esClient.get(g -> g.index("products").id("bk-1"), Product.class);
    if (getResponse.found()) {
      Product pdt = getResponse.source();
      LOG.info("Product name {}", pdt.getName());
    } else {
      LOG.info("Product not found.");
    }

    GetResponse<ObjectNode> objectNodeGetResponse = esClient.get(g -> g.index("products").id("bk-1"), ObjectNode.class);
    LOG.info("Response of getting products is {}", objectNodeGetResponse);

    // 4. Search documents
    String searchText = "bike";
    SearchResponse<Product> searchResponse =
        esClient.search(
            s -> s.index("products").query(q -> q.match(t -> t.field("name").query(searchText))),
            Product.class);
    LOG.info("Response of searching documents are {}", searchResponse);

    // 5. Delete documents
    DeleteResponse deleteResponse = esClient.delete(d -> d.index("products").id("bk-1"));
    LOG.info("Response of deleting documents is {}", deleteResponse);

    // 6. Delete an index
    DeleteIndexResponse deleteIndexResponse = esClient.indices().delete(d -> d.index("products"));
    LOG.info("Response of deleting an index is {}", deleteIndexResponse);
  }
}
