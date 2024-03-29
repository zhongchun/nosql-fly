package com.bermaker.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EsDemo {

  private static Logger LOG = LoggerFactory.getLogger(EsDemo.class);

  public static void main(String[] args) {

    System.setProperty(
        "javax.net.ssl.trustStore", "/Users/zhongchun/Tools/elasticsearch/mykeystore.jks");
    System.setProperty("javax.net.ssl.trustStorePassword", "");

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
    ElasticsearchClient esClient = new ElasticsearchClient(transport);

    try {
      // 1. Create an index
      CreateIndexResponse createIndexResponse = esClient.indices().create(c -> c.index("products"));
      LOG.info("Response of creating an index is {}", createIndexResponse);

      // 2. Ingest data and index a document
      Product product = new Product("bk-1", "City bike", 123.0F);
      IndexResponse indexResponse =
          esClient.index(i -> i.index("products").id(product.getSku()).document(product));
      LOG.info("Indexed with version {}", indexResponse.version());

      // 3. Get documents
      GetResponse<Product> response =
          esClient.get(g -> g.index("products").id("bk-1"), Product.class);
      if (response.found()) {
        Product pdt = response.source();
        LOG.info("Product name {}", pdt.getName());
      } else {
        LOG.info("Product not found.");
      }

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
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } finally {
      try {
        restClient.close();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }
}
