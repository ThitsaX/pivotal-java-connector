package com.thitsaworks.mojaloop.coreconnector.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thitsaworks.mojaloop.coreconnector.CoreConnectorConfiguration;
import com.thitsaworks.mojaloop.coreconnector.audit.AuditPublisherService;
import com.thitsaworks.mojaloop.coreconnector.audit.BackendErrorSerializer;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.ErrorCode;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.StateEnum;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Currency;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformation;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ExtensionList;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Money;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Party;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartyIdInfo;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartyIdType;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransfersIDPatchResponse;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.PendingTransfer;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.PendingTransfersStore;
import com.thitsaworks.mojaloop.coreconnector.logging.MdcExtractors;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsPullListener;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsService;
import com.thitsaworks.mojaloop.coreconnector.payload.fspclient.ConfirmationForTransfer;
import com.thitsaworks.mojaloop.coreconnector.payload.nats.PatchTransfersNatsMessage;
import com.thitsaworks.mojaloop.coreconnector.services.FspClientService;
import io.nats.client.JetStreamManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class PatchTransfersListener implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(PatchTransfersListener.class);

    private static final String COMMITTED = "COMMITTED";

    private final NatsService natsService;

    private final PendingTransfersStore pendingStore;

    private final FspClientService fspClientService;

    private final CoreConnectorConfiguration.Settings config;

    private final ObjectMapper objectMapper;

    private final AuditPublisherService auditPublisher;

    private final BackendErrorSerializer backendErrorSerializer;

    private NatsPullListener<PatchTransfersNatsMessage> listener;

    public PatchTransfersListener(NatsService natsService,
                                  PendingTransfersStore pendingStore,
                                  FspClientService fspClientService,
                                  CoreConnectorConfiguration.Settings config,
                                  ObjectMapper objectMapper,
                                  AuditPublisherService auditPublisher,
                                  BackendErrorSerializer backendErrorSerializer) {

        this.natsService = natsService;
        this.pendingStore = pendingStore;
        this.fspClientService = fspClientService;
        this.config = config;
        this.objectMapper = objectMapper;
        this.auditPublisher = auditPublisher;
        this.backendErrorSerializer = backendErrorSerializer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        String connectorId = config.getConnectorId();
        String subject = natsService.getPatchTransfersSubject();
        String durable = natsService.normalizeDurable(connectorId, "connector-consumer-patch-transfers");

        JetStreamManagement jsm = natsService.jetstreamManager();
        String stream = natsService.ensureStream(jsm, subject);
        natsService.ensureConsumer(jsm, stream, durable, subject);

        LOG.info("Listening on '{}'", subject);

        listener = new NatsPullListener<>(LOG,
                                          natsService,
                                          "PatchTransfers",
                                          PatchTransfersNatsMessage.class,
                                          this::handle,
                                          MdcExtractors::patchTransfers);
        listener.start(subject, stream, durable, "patch-transfers-listener");
    }

    @Override
    public void destroy() {

        if (listener != null) {
            listener.stop();
        }
    }

    private void handle(PatchTransfersNatsMessage msg) throws Exception {

        String transferId = msg.getTransferId();
        String transferState = transferState(msg.getResponse());

        LOG.info("Put Transfer Request from Inbound to Payee cc for TransferId {} : {}",
                 transferId,
                 this.objectMapper.writeValueAsString(msg));
        boolean acquired = pendingStore.acquireLock(transferId);
        if (!acquired) {
            LOG.warn("patchTransfers transferId={} lock not acquired, another instance is handling it", transferId);
            return;
        }

        PendingTransfer pending = null;
        try {
            pending = pendingStore.get(transferId);

            if (pending == null) {
                LOG.warn("patchTransfers transferId={} not found in pending store - skipping", transferId);
                return;
            }

            if (!COMMITTED.equals(transferState)) {
                LOG.warn("patchTransfers transferId={} state={} - skipping confirmation", transferId, transferState);
                pendingStore.delete(transferId);
                return;
            }
            StateEnum confirmationState = confirmationState(transferState);
            String confirmedHomeTransactionId = confirmTransfer(transferId,
                                                                pending.payeeMobile(),
                                                                pending.payerMobile(),
                                                                pending.amount(),
                                                                pending.payeeReceiveAmount(),
                                                                pending.currency(),
                                                                pending.homeTransactionId(),
                                                                pending.payerFspId(),
                                                                pending.payeeFspId(),
                                                                pending.note(),
                                                                confirmationState,
                                                                pending.extensionList());

            pendingStore.delete(transferId);

            LOG.info("patchTransfers CONFIRMED transferId={} payeeMobile={} amount={} {} homeTransactionId={}",
                     transferId,
                     pending.payeeMobile(),
                     pending.amount(),
                     pending.currency(),
                     confirmedHomeTransactionId);
        } catch (Exception err) {
            ErrorInformationResponse errorResponse = toErrorResponse(err, transferId);

            publishPatchErrorAudit(msg, pending, err);
            pendingStore.delete(transferId);

            LOG.error("patchTransfers transferId={} confirmation failed: {}",
                      transferId,
                      objectMapper.writeValueAsString(errorResponse),
                      err);
        } finally {
            pendingStore.releaseLock(transferId);
        }
    }

    private String confirmTransfer(String transferId,
                                   String payeeMobile,
                                   String payerMobile,
                                   String amount,
                                   Money payeeReceiveAmount,
                                   String currency,
                                   String homeTransactionId,
                                   String payerFspId,
                                   String payeeFspId,
                                   String note,
                                   StateEnum confirmationState,
                                   ExtensionList extensionList) throws JsonProcessingException, PutTransferException {

        LOG.info(
            "confirmTransfer transferId={} payeeMobile={} amount={} payeeReceiveAmount={} homeTransactionId={} state={}",
            transferId,
            payeeMobile,
            amount,
            payeeReceiveAmount,
            homeTransactionId,
            confirmationState);

        maybeForceCreditFailure(transferId);

        ConfirmationForTransfer.Request request = new ConfirmationForTransfer.Request();
        request.setTransferId(transferId);
        request.setHomeTransactionId(homeTransactionId);
        request.setQuoteRequest(quoteRequest(payeeMobile,
                                             payerMobile,
                                             amount,
                                             payeeReceiveAmount,
                                             currency,
                                             payerFspId,
                                             payeeFspId,
                                             note));
        request.setQuote(quote(extensionList));
        request.setCurrentState(confirmationState);
        ConfirmationForTransfer.Response response = this.fspClientService.doConfirmationForTransfer(request);

        if (response == null) {
            throw new PatchTransfersListener.PutTransferException(String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode()),
                                                                  "No response from DFSP backend for transferId=" +
                                                                      transferId);
        }

        if (response.getError() != null) {
            var errorInformation = response.getError();
            throw new PatchTransfersListener.PutTransferException(errorInformation.getStatusCode(),
                                                                  errorInformation.getMessage());

        }

        String confirmedHomeTransactionId = response.getHomeTransactionId();
        if (confirmedHomeTransactionId == null || confirmedHomeTransactionId.isBlank()) {
            throw new PatchTransfersListener.PutTransferException(String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode()),
                                                                  "DFSP backend confirmation returned empty homeTransactionId for transferId=" +
                                                                      transferId);
        }

        LOG.info("Put transfer response from Payee for TransferId {} : {}",
                 transferId,
                 this.objectMapper.writeValueAsString(response));
        return confirmedHomeTransactionId;
    }

    private void maybeForceCreditFailure(String transferId) {

        if (!config.isConnectorForcePatchError()) {
            return;
        }

        Map<String, Object> responseBody = new LinkedHashMap<>();
        responseBody.put("errorCode", "WALLET_LOCKED");
        responseBody.put("errorMessage", "Simulated credit failure for transferId=" + transferId);

        throw new BackendErrorSerializer.SimulatedBackendException(500,
                                                                   "ERR_BAD_RESPONSE",
                                                                   "Request failed with status code 500",
                                                                   responseBody);
    }

    private void publishPatchErrorAudit(PatchTransfersNatsMessage msg, PendingTransfer pending, Exception err) {

        if (pending == null) {
            return;
        }

        try {
            Map<String, Object> context = new LinkedHashMap<>();
            context.put("transferId", msg.getTransferId());
            context.put("payeeMobile", pending.payeeMobile());
            context.put("amount", pending.amount());
            context.put("currency", pending.currency());

            String
                serialized =
                backendErrorSerializer.serialize(new BackendErrorSerializer.BackendErrorInput(this.config.getConnectorId(),
                                                                                              "confirmTransfer",
                                                                                              err,
                                                                                              context));

            auditPublisher.publishPatchError(new AuditPublisherService.PatchErrorInput(msg.getTransferId(),
                                                                                       msg.getPayerFsp(),
                                                                                       msg.getPayeeFsp(),
                                                                                       serialized));
        } catch (Exception publishErr) {
            LOG.error("Failed to publish PATCH ERROR audit for transferId={}", msg.getTransferId(), publishErr);
        }
    }

    private ConfirmationForTransfer.QuoteRequest quoteRequest(String payeeMobile,
                                                              String payerMobile,
                                                              String amount,
                                                              Money payeeReceiveAmount,
                                                              String currency,
                                                              String payerFspId,
                                                              String payeeFspId,
                                                              String note) {

        ConfirmationForTransfer.Body body = new ConfirmationForTransfer.Body();
        body.setPayer(party(payerMobile, payerFspId));
        body.setPayee(party(payeeMobile, payeeFspId));
        body.setAmount(amount(amount, currency));
        body.setPayeeReceiveAmount(new BigDecimal(payeeReceiveAmount.getAmount()));
        body.setNote(note);
        ConfirmationForTransfer.QuoteRequest quoteRequest = new ConfirmationForTransfer.QuoteRequest();
        quoteRequest.setBody(body);
        return quoteRequest;
    }

    private Party party(String identifier, String fspId) {

        PartyIdInfo partyIdInfo = new PartyIdInfo();
        partyIdInfo.setPartyIdentifier(identifier);
        partyIdInfo.setFspId(fspId);

        Party party = new Party();
        party.setPartyIdInfo(partyIdInfo);
        return party;
    }

    private ConfirmationForTransfer.Quote quote(ExtensionList extensionList) {

        ConfirmationForTransfer.InternalResponse response = new ConfirmationForTransfer.InternalResponse();
        response.setExtensionList(extensionList);

        ConfirmationForTransfer.Quote quote = new ConfirmationForTransfer.Quote();
        quote.setResponse(response);
        return quote;
    }

    private Money amount(String value, String currency) {

        Money money = new Money();
        money.setAmount(value);
        money.setCurrency(Currency.fromValue(currency));
        return money;
    }

    private String transferState(TransfersIDPatchResponse response) {

        return response == null || response.getTransferState() == null ? null : response.getTransferState()
                                                                                        .toString();
    }

    private static final class PutTransferException extends Exception {

        private final String errorCode;

        private PutTransferException(String errorCode, String message) {

            super(message);
            this.errorCode = errorCode;
        }

        private String errorCode() {

            return errorCode;
        }

    }

    private ErrorInformationResponse toErrorResponse(Exception err, String idValue) throws JsonProcessingException {

        String errorCode = String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode());
        String errorDescription = err.getMessage();

        if (err instanceof PatchTransfersListener.PutTransferException putTransferException) {
            errorCode = putTransferException.errorCode();
            errorDescription = putTransferException.getMessage();
        }

        ErrorInformation
            errorInformation =
            new ErrorInformation().errorCode(errorCode)
                                  .errorDescription(errorDescription);
        ErrorInformationResponse errorInformationResponse = new ErrorInformationResponse().errorInformation(
            errorInformation);
        LOG.error("Patch Transfer error Response from Payee cc to HUB for idValue {} : {}",
                  idValue,
                  this.objectMapper.writeValueAsString(errorInformationResponse));

        return errorInformationResponse;
    }
    private StateEnum confirmationState(String transferState) {

        return COMMITTED.equals(transferState) ? StateEnum.COMPLETED : StateEnum.valueOf(transferState);
    }

}
