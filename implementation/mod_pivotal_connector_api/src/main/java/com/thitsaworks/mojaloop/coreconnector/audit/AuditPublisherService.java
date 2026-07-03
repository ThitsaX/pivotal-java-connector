/*
 * Copyright (c) 2026 ThitsaWorks
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
package com.thitsaworks.mojaloop.coreconnector.audit;

import com.thitsaworks.mojaloop.coreconnector.CoreConnectorConfiguration;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsService;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AuditPublisherService {

    private static final Logger LOG = LoggerFactory.getLogger(AuditPublisherService.class);

    private static final String SUBJECT = "audit.transaction";

    private static final String STREAM_SUBJECT = "audit.>";

    private static final String PATCH_PHASE = "PATCH";

    private static final String ERROR_ACTION = "ERROR";

    private static final String SUCCESS_ACTION = "RESPONSE";

    private static final String CONNECTOR_GATEWAY = "CONNECTOR";

    private final NatsService natsService;

    private final CoreConnectorConfiguration.Settings config;

    private boolean streamResolved;

    public AuditPublisherService(NatsService natsService, CoreConnectorConfiguration.Settings config) {

        this.natsService = natsService;
        this.config = config;
    }

    public void publishPatchError(PatchErrorInput input) throws Exception {

        ensureStream();

        Map<String, Object> content = new LinkedHashMap<>();
        content.put("correlationId", input.correlationId());
        content.put("payerFspId", input.payerFsp());
        content.put("payeeFspId", input.payeeFsp());
        content.put("error", input.error());
        content.put("occurredAt", Instant.now().toString());

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("phase", PATCH_PHASE);
        message.put("action", ERROR_ACTION);
        message.put("gateway", CONNECTOR_GATEWAY);
        message.put("content", content);

        this.natsService.jetstream().publish(SUBJECT, this.natsService.serialize(message));

        LOG.info(
            "Published PATCH ERROR audit transferId={} payerFspId={} payeeFspId={}",
            input.correlationId(), input.payerFsp(), input.payeeFsp());
    }

    public void publishPatchSuccess(PatchSuccessInput input) throws Exception {

        ensureStream();

        Map<String, Object> content = new LinkedHashMap<>();
        content.put("correlationId", input.correlationId());
        content.put("payerFsp", input.payerFsp());
        content.put("payeeFsp", input.payeeFsp());
        content.put("payeeMobile", input.payeeMobile());
        content.put("amount", input.amount());
        content.put("homeTransactionId", input.homeTransactionId());
        content.put("occurredAt", Instant.now().toString());


        Map<String, Object> message = new LinkedHashMap<>();
        message.put("phase", PATCH_PHASE);
        message.put("action",SUCCESS_ACTION );
        message.put("gateway", CONNECTOR_GATEWAY);
        message.put("content", content);

        this.natsService.jetstream().publish(SUBJECT, this.natsService.serialize(message));

        LOG.info(
                "Published PATCH SUCCESS audit transferId={} payeeMobile={} amount={} homeTransactionId={}",
                input.correlationId, input.payeeMobile(), input.amount(), input.homeTransactionId());
    }

    private synchronized void ensureStream() throws Exception {

        if (streamResolved) {
            return;
        }

        JetStreamManagement jsm = this.natsService.jetstreamManager();
        if (!jsm.getStreamNamesBySubjectFilter(SUBJECT).isEmpty()) {
            streamResolved = true;
            return;
        }

        try {
            StreamConfiguration streamConfig = StreamConfiguration
                                                   .builder()
                                                   .name(config.getPivotalAuditStreamName())
                                                   .subjects(List.of(STREAM_SUBJECT))
                                                   .storageType(StorageType.File)
                                                   .build();

            jsm.addStream(streamConfig);
            LOG.info(
                "Created stream '{}' with subjects '{}'", config.getPivotalAuditStreamName(),
                STREAM_SUBJECT);
        } catch (JetStreamApiException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();

            if (!msg.contains("stream name already in use")) {
                throw e;
            }

            LOG.debug("Stream '{}' already exists", config.getPivotalAuditStreamName());
        }

        streamResolved = true;
    }

    public record PatchErrorInput(String correlationId,
                                  String payerFsp,
                                  String payeeFsp,
                                  String error) { }

    public record PatchSuccessInput(String correlationId,
                                    String payerFsp,
                                    String payeeFsp,
                                    String payeeMobile,
                                    String amount,
                                    String homeTransactionId) { }

}
