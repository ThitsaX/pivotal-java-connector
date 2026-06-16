package com.thitsaworks.mojaloop.coreconnector.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartiesTypeIDPutResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.QuotesIDPutResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransfersIDPutResponse;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class FspiopCallbackService {

    private static final Logger log = LoggerFactory.getLogger(FspiopCallbackService.class);

    private static final String CT_PARTIES = "application/vnd.interoperability.parties+json;version=2.0";

    private static final String CT_QUOTES = "application/vnd.interoperability.quotes+json;version=2.0";

    private static final String CT_TRANSFERS = "application/vnd.interoperability.transfers+json;version=2.0";

    private static final Pattern TRACEPARENT = Pattern.compile(
        "^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

    private static final DateTimeFormatter HTTP_DATE_FORMATTER = DateTimeFormatter
                                                                     .ofPattern(
                                                                         "EEE, dd MMM yyyy HH:mm:ss 'GMT'",
                                                                         Locale.US)
                                                                     .withZone(ZoneOffset.UTC);

    private final OkHttpClient http;

    private final ObjectMapper objectMapper;

    public FspiopCallbackService(OkHttpClient http, ObjectMapper objectMapper) {

        this.http = http;
        this.objectMapper = objectMapper;
    }

    public void putParties(String baseUrl,
                           String traceparent,
                           String source,
                           String destination,
                           String partyIdType,
                           String partyId,
                           PartiesTypeIDPutResponse body,
                           String subId) throws Exception {

        String url = partiesUrl(baseUrl, partyIdType, partyId, subId, false);
        put(url, body, headers(CT_PARTIES, traceparent, source, destination, true));
    }

    public void putPartiesError(String baseUrl,
                                String traceparent,
                                String source,
                                String destination,
                                String partyIdType,
                                String partyId,
                                ErrorInformationResponse error,
                                String subId) throws Exception {

        String url = partiesUrl(baseUrl, partyIdType, partyId, subId, true);
        put(url, error, headers(CT_PARTIES, traceparent, source, destination, true));
    }

    private String partiesUrl(String baseUrl,
                              String partyIdType,
                              String partyId,
                              String subId,
                              boolean error) {

        String url =
            subId == null || subId.isBlank() ? baseUrl + "/parties/" + partyIdType + "/" + partyId :
                baseUrl + "/parties/" + partyIdType + "/" + partyId + "/" + subId;
        return error ? url + "/error" : url;
    }

    public void putQuotes(String baseUrl,
                          String traceparent,
                          String source,
                          String destination,
                          String quoteId,
                          QuotesIDPutResponse body) throws Exception {

        put(
            baseUrl + "/quotes/" + quoteId, body,
            headers(CT_QUOTES, traceparent, source, destination, false));
    }

    public void putQuotesError(String baseUrl,
                               String traceparent,
                               String source,
                               String destination,
                               String quoteId,
                               ErrorInformationResponse error) throws Exception {

        put(
            baseUrl + "/quotes/" + quoteId + "/error", error,
            headers(CT_QUOTES, traceparent, source, destination, false));
    }

    public void putTransfers(String baseUrl,
                             String traceparent,
                             String source,
                             String destination,
                             String transferId,
                             TransfersIDPutResponse body) throws Exception {

        put(
            baseUrl + "/transfers/" + transferId, body,
            headers(CT_TRANSFERS, traceparent, source, destination, false));
    }

    public void putTransfersError(String baseUrl,
                                  String traceparent,
                                  String source,
                                  String destination,
                                  String transferId,
                                  ErrorInformationResponse error) throws Exception {

        put(
            baseUrl + "/transfers/" + transferId + "/error", error,
            headers(CT_TRANSFERS, traceparent, source, destination, false));
    }

    private Map<String, String> headers(String contentType,
                                        String traceparent,
                                        String source,
                                        String destination,
                                        boolean includeAccept) {

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-type", contentType);
        headers.put("date", HTTP_DATE_FORMATTER.format(Instant.now()));
        headers.put("fspiop-source", source);

        if (includeAccept) {
            headers.put("accept", contentType);
        }
        if (destination != null && !destination.isBlank()) {
            headers.put("fspiop-destination", destination);
        }

        if (isValidTraceparent(traceparent)) {
            headers.put("traceparent", traceparent);
        }

        return headers;
    }

    private boolean isValidTraceparent(String value) {

        if (value == null || value.isBlank()) {
            return false;
        }
        return TRACEPARENT.matcher(value).matches(); // remove the zero-rejection
    }

    private void put(String url, Object body, Map<String, String> headers) throws Exception {

        byte[] jsonBytes = objectMapper.writeValueAsBytes(body);

        log.debug("PUT {} body={}", url, new String(jsonBytes, StandardCharsets.UTF_8));

        RequestBody requestBody = new RequestBody() {

            @Override
            public MediaType contentType() {

                return null;
            }

            @Override
            public long contentLength() {

                return jsonBytes.length;
            }

            @Override
            public void writeTo(BufferedSink sink) throws java.io.IOException {

                sink.write(jsonBytes);
            }
        };

        Request.Builder builder = new Request.Builder().url(url).put(requestBody);
        headers.forEach(builder::header);

        try (Response response = http.newCall(builder.build()).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                log.error("PUT {} failed: HTTP {} body={}", url, response.code(), responseBody);
                throw new IllegalStateException(
                    "FSPIOP callback failed HTTP " + response.code() + " for " + url + ": " +
                        responseBody);
            }
        }
    }

}
