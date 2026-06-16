package com.thitsaworks.mojaloop.coreconnector.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thitsaworks.mojaloop.coreconnector.CoreConnectorConfiguration;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.ErrorCode;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformation;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartiesTypeIDPutResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Party;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartyIdInfo;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartyIdType;
import com.thitsaworks.mojaloop.coreconnector.logging.MdcExtractors;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsPullListener;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsService;
import com.thitsaworks.mojaloop.coreconnector.payload.fspclient.LookUp;
import com.thitsaworks.mojaloop.coreconnector.payload.nats.GetPartiesNatsMessage;
import com.thitsaworks.mojaloop.coreconnector.services.FspClientService;
import io.nats.client.JetStreamManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class PartiesListener implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(PartiesListener.class);

    private final NatsService natsService;

    private final FspiopCallbackService callback;

    private final FspClientService fspClientService;

    private final CoreConnectorConfiguration.Settings config;

    private final ObjectMapper objectMapper;

    private NatsPullListener<GetPartiesNatsMessage> listener;

    public PartiesListener(NatsService natsService,
                           FspiopCallbackService callback,
                           FspClientService fspClientService,
                           CoreConnectorConfiguration.Settings config,
                           ObjectMapper objectMapper) {

        this.natsService = natsService;
        this.callback = callback;
        this.fspClientService = fspClientService;
        this.config = config;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        String connectorId = config.getConnectorId();
        String subject = natsService.getPartiesSubject();
        String durable = natsService.normalizeDurable(
            connectorId, "connector-consumer-get-parties");

        JetStreamManagement jsm = natsService.jetstreamManager();
        String stream = natsService.ensureStream(jsm, subject);
        natsService.ensureConsumer(jsm, stream, durable, subject);

        LOG.info("Listening on '{}'", subject);

        listener = new NatsPullListener<>(
            LOG, natsService, "GetParties",
            GetPartiesNatsMessage.class, data -> handle(data, connectorId),
            MdcExtractors::getParties);
        listener.start(subject, stream, durable, "parties-listener");
    }

    @Override
    public void destroy() {

        if (listener != null) {
            listener.stop();
        }
    }

    private void handle(GetPartiesNatsMessage msg, String connectorId) throws Exception {

        String correlationId = msg.getCorrelationId();
        String payerFsp = msg.getPayerFsp();
        String partyIdType = msg.getPartyIdType();
        String partyId = msg.getPartyId();
        String subId = msg.getSubId() == null || msg.getSubId().isBlank() ? null : msg.getSubId();

        LOG.info(
            "Get Party request info from the inbound to Payee cc: {}, {}", partyIdType, partyId);

        try {
            LookUp.Request request = new LookUp.Request();
            request.setIdType(PartyIdType.fromValue(partyIdType));
            request.setIdValue(partyId);

            LookUp.Response lookupResponse = fspClientService.doLookUp(request);
            if (lookupResponse == null) {
                throw new PartyLookupException(
                    String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode()),
                    "No response from DFSP backend for partyId=" + partyId);
            }
            if (lookupResponse.getError() != null &&
                    lookupResponse.getError().getErrorInformation() != null) {
                var errorInformation = lookupResponse.getError().getErrorInformation();
                throw new PartyLookupException(
                    errorInformation.getStatusCode(), errorInformation.getMessage());
            }
            PartyIdInfo partyIdInfo = new PartyIdInfo()
                                          .partyIdType(lookupResponse.getIdType())
                                          .partyIdentifier(lookupResponse.getIdValue())
                                          .fspId(connectorId)
                                          .partySubIdOrType(lookupResponse.getIdSubValue());

            String name =
                lookupResponse.getDisplayName() != null ? lookupResponse.getDisplayName() :
                    lookupResponse.getFirstName() + " " + lookupResponse.getMiddleName() + " " +
                        lookupResponse.getLastName();

            Party party = new Party()
                              .partyIdInfo(partyIdInfo)
                              .name(name)
                              .merchantClassificationCode(
                                  lookupResponse.getMerchantClassificationCode())
                              .supportedCurrencies(lookupResponse.getSupportedCurrencies());
            PartiesTypeIDPutResponse callbackResponse = new PartiesTypeIDPutResponse();
            callbackResponse.setParty(party);
            LOG.info(
                "LookUp response from Payee for idValue {} : {}", lookupResponse.getIdValue(),
                this.objectMapper.writeValueAsString(lookupResponse));

            callback.putParties(
                config.getPartiesUrl(), correlationId, connectorId, payerFsp, partyIdType, partyId,
                callbackResponse, subId);

            LOG.info(
                "Get Party Response from Payee cc to hub for idValue {} : {}",
                lookupResponse.getIdValue(), this.objectMapper.writeValueAsString(lookupResponse));
        } catch (Exception err) {
            callback.putPartiesError(
                config.getPartiesUrl(), correlationId, connectorId, payerFsp, partyIdType, partyId,
                toErrorResponse(err, partyId), subId);
        }
    }

//  Error Handling

    private ErrorInformationResponse toErrorResponse(Exception err, String idValue)
        throws JsonProcessingException {

        String errorCode = String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode());
        String errorDescription = err.getMessage();

        if (err instanceof PartyLookupException lookupException) {
            errorCode = lookupException.errorCode();
            errorDescription = lookupException.getMessage();
        } else if (err instanceof IllegalArgumentException) {
            errorCode = String.valueOf(ErrorCode.GENERIC_VALIDATION_ERROR.getStatusCode());
        }

        if (errorDescription == null || errorDescription.isBlank()) {
            errorDescription = ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getDefaultMessage();
        }

        if (errorCode == null || errorCode.isBlank()) {
            errorCode = String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode());
        }

        ErrorInformation errorInformation = new ErrorInformation()
                                                .errorCode(errorCode)
                                                .errorDescription(errorDescription);
        ErrorInformationResponse errorInformationResponse = new ErrorInformationResponse().errorInformation(
            errorInformation);
        LOG.error(
            "Get Party error Response from Payee cc to HUB for idValue {} : {}", idValue,
            this.objectMapper.writeValueAsString(errorInformationResponse));

        return errorInformationResponse;
    }

    private static final class PartyLookupException extends Exception {

        private final String errorCode;

        private PartyLookupException(String errorCode, String message) {

            super(message);
            this.errorCode = errorCode;
        }

        private String errorCode() {

            return errorCode;
        }

    }

}
