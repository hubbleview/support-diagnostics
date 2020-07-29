package com.elastic.support.rest;

import com.elastic.support.Constants;
import com.elastic.support.util.SystemProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

public class RestClient implements Closeable {

    private static final Logger logger = LogManager.getLogger(RestClient.class);
    private CloseableHttpClient client;
    private HttpHost httpHost;
    private HttpClientContext httpContext;
    private HttpClientConnectionManager manager = new BasicHttpClientConnectionManager();

    public RestResult execQuery(String url) {
        return new RestResult(execGet(url), url);
    }

    public RestResult execQuery(String url, String fileName) {
        return new RestResult(execGet(url), fileName, url);
    }

    public HttpResponse execGet(String query) {
        HttpGet httpGet = new HttpGet(query);
        logger.debug(query);
        return execRequest(httpGet);
    }

    private HttpResponse execRequest(HttpRequestBase httpRequest) {
        try {
            return client.execute(httpHost, httpRequest, httpContext);
        } catch (HttpHostConnectException e) {
            logger.error("Host connection error.", e);
            throw new RuntimeException("Host connection");
        } catch (Exception e) {
            logger.error("Unexpected Execution Error", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public HttpResponse execPost(String uri, String payload) {
        try {
            HttpPost httpPost = new HttpPost(uri);
            StringEntity entity = new StringEntity(payload);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            logger.debug(uri + SystemProperties.fileSeparator + payload);
            return execRequest(httpPost);
        } catch (UnsupportedEncodingException e) {
            logger.error(Constants.CONSOLE, "Error with json body.", e);
            throw new RuntimeException("Could not complete post request.");
        } finally {
            manager.closeExpiredConnections();
            manager.closeIdleConnections(5, TimeUnit.SECONDS);
        }
    }

    public HttpResponse execDelete(String uri) {
        HttpDelete httpDelete = new HttpDelete(uri);
        logger.debug(uri);

        return execRequest(httpDelete);
    }

    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            logger.error("Error occurred closing client connection.");
        }
    }

    public RestClient (
            String host,
            int port,
            String scheme,
            String user,
            String password,
            String proxyHost,
            int proxyPort,
            String proxyUser,
            String proxyPassword,
            String pkiKeystore,
            String pkiKeystorePass,
            boolean bypassVerify,
            int connectionTimeout,
            int connectionRequestTimeout,
            int socketTimeout) {

        try {
            HttpClientBuilder clientBuilder = HttpClients.custom();
            httpHost = new HttpHost(host, port, scheme);
            HttpHost httpProxyHost = null;
            httpContext = HttpClientContext.create();

            // Create AuthCache instance
            AuthCache authCache = new BasicAuthCache();
            // Generate BASIC scheme object and add it to the local auth cache
            BasicScheme basicAuth = new BasicScheme();

            clientBuilder.setDefaultRequestConfig(RequestConfig.custom()
                    .setCookieSpec(CookieSpecs.STANDARD)
                    .setConnectTimeout(connectionTimeout)
                    .setSocketTimeout(socketTimeout)
                    .setConnectionRequestTimeout(connectionRequestTimeout).build())
                    .setConnectionManager(manager);

            // If there's a proxy server, set it now.
            if (StringUtils.isNotEmpty(proxyHost)) {
                httpProxyHost = new HttpHost(proxyHost, proxyPort);
                clientBuilder.setProxy(httpProxyHost);
                basicAuth.processChallenge(new BasicHeader(AUTH.PROXY_AUTH, "BASIC realm=default"));
                authCache.put(httpProxyHost, basicAuth);
            } else {
                authCache.put(httpHost, basicAuth);
            }

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);

            // If authentication was supplied
            if (StringUtils.isNotEmpty(user) && StringUtils.isEmpty(proxyUser)) {
                httpContext.setAuthCache(authCache);
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            } else if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(proxyUser)) {
                httpContext.setAuthCache(authCache);
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(proxyUser, proxyPassword));
            } else if (StringUtils.isNotEmpty(proxyUser)) {
                httpContext.setAuthCache(authCache);
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(proxyUser, proxyPassword));
            }

            SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
            sslContextBuilder.loadTrustMaterial(new TrustAllStrategy());
            if (StringUtils.isNotEmpty(pkiKeystore)) {
                // If they are using a PKI auth set it up now
                KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                ks.load(new FileInputStream(pkiKeystore), pkiKeystorePass.toCharArray());
                sslContextBuilder.loadKeyMaterial(ks, pkiKeystorePass.toCharArray());
            }

            SSLContext sslCtx = sslContextBuilder.build();

            SSLConnectionSocketFactory factory = null;
            if (bypassVerify) {
                factory = new SSLConnectionSocketFactory(sslCtx, NoopHostnameVerifier.INSTANCE);
            } else {
                factory = new SSLConnectionSocketFactory(sslCtx);
            }
            clientBuilder.setSSLSocketFactory(factory);

            CloseableHttpClient httpClient = clientBuilder.build();

        } catch (Exception e) {
            logger.error("Connection setup failed", e);
            throw new RuntimeException("Error establishing http connection for: " + host, e);
        }
    }
}
