/*
 * Copyright (c) 2024-2026 ThitsaWorks Pte. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thitsaworks.mojaloop.coreconnector.component.retrofit;

import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Converter;
import retrofit2.Retrofit;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RetrofitServiceBuilder<S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetrofitServiceBuilder.class);

    private final Class<S> service;

    private final OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();

    private final Retrofit.Builder retrofitBuilder = new Retrofit.Builder();

    public RetrofitServiceBuilder(Class<S> service, String baseUrl) {

        this.service = service;
        this.retrofitBuilder.baseUrl(baseUrl);

    }

    public RetrofitServiceBuilder<S> withHttpLog(HttpLoggingInterceptor.Level level, boolean enableMasking) {

        HttpLoggingInterceptor.Logger logger;

        if (enableMasking) {
            logger = message -> {
                // First process JSON fields
                String sanitized = message;

                // Mask credentials in JSON request bodies. PIN values are always fully hidden.
                Pattern
                    jsonPattern =
                    Pattern.compile("(?i)(\"([^\"]+)\"\\s*:\\s*\")([^\"]*)(\")");
                Matcher jsonMatcher = jsonPattern.matcher(message);
                StringBuffer jsonResult = new StringBuffer();
                while (jsonMatcher.find()) {
                    String fieldName = jsonMatcher.group(2);
                    String value = jsonMatcher.group(3);
                    String masked = value;

                    if (fieldName != null && fieldName.equalsIgnoreCase("username")) {
                        masked = maskUsername(value);
                    } else if (fieldName != null && fieldName.matches("(?i)user|pwd")) {
                        masked = value.length() > 3
                                     ? "****" + value.substring(value.length() - 3)
                                     : "****";
                    } else if (fieldName != null && fieldName.matches("(?i)pincode|pinCode")) {
                        masked = "****";
                    } else if (fieldName != null && fieldName.matches("(?i)access_token|password")) {
                        masked = "****";
                    }

                    jsonMatcher.appendReplacement(jsonResult, jsonMatcher.group(1) + masked + jsonMatcher.group(4));
                }
                jsonMatcher.appendTail(jsonResult);
                sanitized = jsonResult.toString();

                // Then process query parameters
                Pattern paramPattern = Pattern.compile(
                    "(?i)((?:auth:(?:user|pwd)|X-PI-Client-Id|X-PI-Client-Secret|grant_type)=)([^&\\s]*)"
                                                      );
                Matcher paramMatcher = paramPattern.matcher(sanitized);
                StringBuffer paramResult = new StringBuffer();
                while (paramMatcher.find()) {
                    String key = paramMatcher.group(1);
                    String value = paramMatcher.group(2);
                    String masked;

                    if (key != null && key.matches("(?i)grant_type=")) {
                        masked = "****";
                    } else {
                        masked = value.length() > 3
                                     ? "****" + value.substring(value.length() - 3)
                                     : "****";
                    }

                    paramMatcher.appendReplacement(paramResult, "$1" + masked);
                }
                paramMatcher.appendTail(paramResult);
                sanitized = paramResult.toString();

                LOGGER.info(sanitized);
            };
        } else {
            logger = LOGGER::info;
        }

        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor(logger);
        loggingInterceptor.setLevel(level);
        loggingInterceptor.redactHeader("Authorization");
        loggingInterceptor.redactHeader("X-PI-Client-Id");
        loggingInterceptor.redactHeader("X-PI-Client-Secret");
        loggingInterceptor.redactHeader("secret-key");
        loggingInterceptor.redactHeader("secret-id");
        loggingInterceptor.redactHeader("currentMemberId");

        this.httpClientBuilder.addInterceptor(loggingInterceptor);

        return this;
    }

    private static String maskUsername(String value) {

        if (value == null || value.isEmpty()) {
            return value;
        }

        if (value.length() <= 3) {
            return "*".repeat(value.length());
        }

        return "*".repeat(value.length() - 3) + value.substring(value.length() - 3);
    }

    public S build() {

        Retrofit retrofit = this.retrofitBuilder.client(this.httpClientBuilder.build())
                                                .build();

        return retrofit.create(this.service);
    }

    public RetrofitServiceBuilder<S> withMutualTLS(InputStream clientCertInputStream,
                                                   String clientCertPassword,
                                                   InputStream trustStoreInputStream,
                                                   String trustStorePassword) {

        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(clientCertInputStream, clientCertPassword.toCharArray());

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, clientCertPassword.toCharArray());

            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(trustStoreInputStream, trustStorePassword.toCharArray());

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS"); // try 1.2 first
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            X509TrustManager tm = (X509TrustManager) tmf.getTrustManagers()[0];

            this.httpClientBuilder.sslSocketFactory(sslContext.getSocketFactory(), tm);

            okhttp3.ConnectionSpec spec = new okhttp3.ConnectionSpec.Builder(okhttp3.ConnectionSpec.MODERN_TLS).tlsVersions(okhttp3.TlsVersion.TLS_1_2).build();
            this.httpClientBuilder.connectionSpecs(List.of(spec));
            this.httpClientBuilder.hostnameVerifier((hostname, session) -> true);

        } catch (Exception e) {
            throw new RuntimeException("Failed to configure mTLS", e);
        }

        return this;
    }


    /** Enable TLS/connection debug using a network interceptor (no EventListener needed). */
    public RetrofitServiceBuilder<S> withTlsDebug() {

        this.httpClientBuilder.addNetworkInterceptor(new HandshakeLoggingNetworkInterceptor());

        return this;
    }

    /** Wrap Dns with debug logs (helps validate host→IP mapping for SNI issues). */
    public RetrofitServiceBuilder<S> withDnsDebug() {

        Dns current = this.httpClientBuilder.build().dns(); // get existing (SYSTEM unless overridden)
        this.httpClientBuilder.dns(new DnsDebug(current));
        return this;
    }

    public RetrofitServiceBuilder<S> withDnsToIpConversion(String dns, String ip) {

        this.httpClientBuilder.dns(host -> {
            if (dns.equals(host)) {
                return java.util.Collections.singletonList(java.net.InetAddress.getByName(ip));
            }
            return okhttp3.Dns.SYSTEM.lookup(host);
        });

        return this;
    }

    public RetrofitServiceBuilder<S> withConverterFactories(Converter.Factory... factories) {

        if (factories != null) {

            for (Converter.Factory factory : factories) {

                this.retrofitBuilder.addConverterFactory(factory);
            }
        }

        return this;
    }

    public RetrofitServiceBuilder<S> withCustomHeaders(Map<String, String> headers) {

        this.httpClientBuilder.addInterceptor(chain -> {

            Request original = chain.request();
            Request.Builder requestBuilder = null;
            requestBuilder = original.newBuilder();

            if (headers != null) {

                for (String key : headers.keySet()) {

                    requestBuilder.header(key, headers.get(key));

                }

            }

            Request request = requestBuilder.build();
            return chain.proceed(request);

        });

        return this;
    }

    public RetrofitServiceBuilder<S> withDisableSSLVerification() {

        // Create a trust manager that does not validate certificate chains
        final TrustManager[] trustManager = new TrustManager[]{
            new X509TrustManager() {

                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType)
                    throws CertificateException {

                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType)
                    throws CertificateException {

                }

                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {

                    return new java.security.cert.X509Certificate[]{};

                }

            }};

        // Install the all-trusting trust manager
        final SSLContext sslContext;

        try {

            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustManager, new java.security.SecureRandom());

            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            this.httpClientBuilder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustManager[0]);
            this.httpClientBuilder.hostnameVerifier(new HostnameVerifier() {

                @Override
                public boolean verify(String hostname, SSLSession session) {

                    return true;

                }

            });

        } catch (KeyManagementException | NoSuchAlgorithmException e) {

            throw new RuntimeException(e);

        }

        return this;
    }

    public RetrofitServiceBuilder<S> withHttpLogging(HttpLoggingInterceptor.Level level, boolean info) {

        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(message -> {
            if (info) {
                LOGGER.info("Retrofit : {}", message);
            } else {
                LOGGER.debug("Retrofit :{}", message);
            }
        });

        logging.setLevel(level);

        this.httpClientBuilder.addInterceptor(logging);

        return this;
    }

    public RetrofitServiceBuilder<S> withInterceptors(Interceptor... interceptors) {

        if (interceptors != null) {

            for (Interceptor interceptor : interceptors) {

                this.httpClientBuilder.addInterceptor(interceptor);
            }
        }

        return this;
    }

    public RetrofitServiceBuilder<S> withTimeouts(int connectTimeout, int callTimeout, int readTimeout) {

        this.httpClientBuilder.connectTimeout(Duration.ofSeconds(connectTimeout <= 0 ? 60 : connectTimeout));
        this.httpClientBuilder.callTimeout(Duration.ofSeconds(callTimeout <= 0 ? 60 : callTimeout));
        this.httpClientBuilder.readTimeout(Duration.ofSeconds(readTimeout <= 0 ? 60 : readTimeout));

        return this;
    }

    private static final class DnsDebug implements Dns {

        private final Dns delegate;

        DnsDebug(Dns delegate) {

            this.delegate = delegate != null ? delegate : Dns.SYSTEM;
        }

        @Override
        public List<InetAddress> lookup(String hostname) throws UnknownHostException {

            LOGGER.info("TLS-DNS start domain={}", hostname);
            try {
                List<InetAddress> out = delegate.lookup(hostname);
                LOGGER.info("TLS-DNS end   domain={} resolved={}", hostname, out);
                return out;
            } catch (UnknownHostException e) {
                LOGGER.error("TLS-DNS fail  domain={} err={}", hostname, e.toString());
                throw e;
            }
        }

    }

    // Logs TLS and connection events without the JVM-wide javax.net.debug noise
    private static final class HandshakeLoggingNetworkInterceptor implements Interceptor {

        @Override
        public Response intercept(Chain chain) throws IOException {

            Request req = chain.request();
            String host = req.url().host();
            Connection connBefore = chain.connection(); // may be null on some paths

            if (connBefore != null) {
                logConn("pre", host, connBefore);
            } else {
                LOGGER.info("TLS-HS pre  host(SNI)={} conn=<none yet>", host);
            }

            Response resp = chain.proceed(req);

            // Prefer response.handshake(); fall back to connection.handshake()
            Handshake hs = resp.handshake();
            if (hs == null && resp.networkResponse() != null) {
                Connection c = chain.connection();
                if (c != null) {
                    hs = c.handshake();
                }
            }

            if (hs != null) {
                LOGGER.info("TLS-HS end  host(SNI)={} version={} cipher={}", host, hs.tlsVersion(), hs.cipherSuite());
                logPeerCerts(hs.peerCertificates());
            } else {
                LOGGER.warn("TLS-HS end  host(SNI)={} (no handshake info)", host);
            }

            Connection connAfter = chain.connection();
            if (connAfter != null) {
                LOGGER.info("TLS-CONN route={} protocol={}", connAfter.route(), connAfter.protocol());
            }

            return resp;
        }
    }
    private static void logConn(String phase, String host, Connection conn) {

        Handshake hs = conn.handshake();
        if (hs != null) {
            LOGGER.info("TLS-HS {}  host(SNI)={} version={} cipher={}", phase, host, hs.tlsVersion(), hs.cipherSuite());
        } else {
            LOGGER.info("TLS-HS {}  host(SNI)={} (no handshake yet)", phase, host);
        }
    }

    @SuppressWarnings("unchecked")
    private static void logPeerCerts(List<Certificate> certs) {

        int i = 0;
        for (Certificate c : certs) {
            if (c instanceof X509Certificate) {
                X509Certificate x = (X509Certificate) c;
                String subj = x.getSubjectX500Principal().getName();
                String issuer = x.getIssuerX500Principal().getName();
                LOGGER.info("TLS-PEER[{}] subject='{}' issuer='{}' notBefore={} notAfter={}", i, subj, issuer, x.getNotBefore(), x.getNotAfter());

                try {
                    Collection<List<?>> sans = x.getSubjectAlternativeNames();
                    if (sans != null) {
                        StringBuilder sb = new StringBuilder();
                        for (List<?> san : sans) {
                            if (san.size() >= 2) {
                                sb.append(san.get(1)).append(", ");
                            }
                        }
                        if (sb.length() > 2) {
                            sb.setLength(sb.length() - 2);
                        }
                        LOGGER.info("TLS-PEER[{}] SANs={}", i, sb);
                    }
                } catch (Exception ignore) { }
            } else {
                LOGGER.info("TLS-PEER[{}] type={}", i, c.getType());
            }
            i++;
        }}


}
