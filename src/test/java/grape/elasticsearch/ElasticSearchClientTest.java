package grape.elasticsearch;

import java.io.IOException;
import java.security.KeyStoreException;
import java.util.Arrays;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author grape
 * @date 2019-08-22
 */
public class ElasticSearchClientTest {

  private static RestClient restClient;

  @BeforeClass
  public static void setup() throws KeyStoreException {
    restClient = RestClient
			.builder(new HttpHost("localhost", 9200, "http"))
			.setRequestConfigCallback(new RequestConfigCallback() {
				@Override
				public Builder customizeRequestConfig(Builder requestConfigBuilder) {
					return requestConfigBuilder.setConnectTimeout(5000)
							.setSocketTimeout(60000);
				}
			})
			.setHttpClientConfigCallback(new HttpClientConfigCallback() {
				@Override
				public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
					return httpClientBuilder
							.setDefaultIOReactorConfig(
									IOReactorConfig
									.custom()
									.setIoThreadCount(1)
									.build());
				}
			})
			.setMaxRetryTimeoutMillis(60000)
			.build();
  }

  @AfterClass
  public static void close() {
    if (null != restClient) {
      try {
        restClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testGetRestClient() {
    assert restClient != null;
  }

  @Test
  public void testPerformRequest() {
    Response response = null;
    try {
      response = restClient.performRequest("GET", "/", new Header[] {});
      Assert.assertNotNull(response);
      System.out.println("requestLine:" + response.getRequestLine());
      System.out.println("host:"+response.getHost());
      System.out.println("statusLine:"+response.getStatusLine());
      System.out.println("headers:"+ Arrays.asList(response.getHeaders()));

      response.getEntity().writeTo(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPerformRequestAsync() {
    restClient.performRequestAsync(
        "GET",
        "/",
        new ResponseListener() {
          @Override
          public void onSuccess(Response response) {
            Assert.assertNotNull(response);
            try {
              response.getEntity().writeTo(System.out);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void onFailure(Exception exception) {
            assert null != exception;
          }
        },
        new Header[] {});
  }
}
