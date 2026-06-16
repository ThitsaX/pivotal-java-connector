package com.thitsaworks.mojaloop.coreconnector.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thitsaworks.mojaloop.coreconnector.CoreConnectorConfiguration;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.ErrorCode;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.FspParty;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformation;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Money;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransferState;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransfersIDPutResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransfersPostRequest;
import com.thitsaworks.mojaloop.coreconnector.ilp.IlpService;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.IlpAgreement;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.PendingTransfer;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.PendingTransfersStore;
import com.thitsaworks.mojaloop.coreconnector.logging.MdcExtractors;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsPullListener;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsService;
import com.thitsaworks.mojaloop.coreconnector.payload.fspclient.ReservationForTransfer;
import com.thitsaworks.mojaloop.coreconnector.payload.nats.PostTransfersNatsMessage;
import com.thitsaworks.mojaloop.coreconnector.services.FspClientService;
import io.nats.client.JetStreamManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class TransfersListener implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(TransfersListener.class);

    private final NatsService natsService;

    private final FspiopCallbackService callback;

    private final IlpService ilp;

    private final PendingTransfersStore pendingStore;

    private final FspClientService fspClientService;

    private final CoreConnectorConfiguration.Settings config;

    private NatsPullListener<PostTransfersNatsMessage> listener;

    private final ObjectMapper objectMapper;

    public TransfersListener(NatsService natsService,
                             FspiopCallbackService callback,
                             IlpService ilp,
                             PendingTransfersStore pendingStore,
                             FspClientService fspClientService,
                             CoreConnectorConfiguration.Settings config,
                             ObjectMapper objectMapper) {

        this.natsService = natsService;
        this.callback = callback;
        this.ilp = ilp;
        this.pendingStore = pendingStore;
        this.fspClientService = fspClientService;
        this.config = config;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        String connectorId = config.getConnectorId();
        String subject = natsService.getTransfersSubject();
        String durable = natsService.normalizeDurable(
            connectorId, "connector-consumer-post-transfers");

        JetStreamManagement jsm = natsService.jetstreamManager();
        String stream = natsService.ensureStream(jsm, subject);
        natsService.ensureConsumer(jsm, stream, durable, subject);

        LOG.info("Listening on '{}'", subject);

        listener = new NatsPullListener<>(
            LOG, natsService, "PostTransfers", PostTransfersNatsMessage.class,
            data -> handle(data, connectorId), MdcExtractors::postTransfers);
        listener.start(subject, stream, durable, "transfers-listener");
    }

    @Override
    public void destroy() {

        if (listener != null) {
            listener.stop();
        }
    }

    private void handle(PostTransfersNatsMessage msg, String connectorId) throws Exception {

        TransfersPostRequest request = msg.getRequest();
        String transferId = request != null ? request.getTransferId() : null;
        LOG.info(
            "Post Transfer request info from the Inbound to Payee cc for TransferId {} : {}",
            request.getTransferId(), this.objectMapper.writeValueAsString(request));

        try {
            if (request == null) {
                throw new IllegalArgumentException("Transfers request is missing");
            }

            String amount = request.getAmount().getAmount();
            String currency = request.getAmount().getCurrency().toString();

            LOG.info("postTransfers transferId={} amount={} {}", transferId, amount, currency);

            if (!connectorId.equals(request.getPayeeFsp())) {
                LOG.warn(
                    "postTransfers transferId={} payeeFsp={} does not match connectorId={} - aborting",
                    transferId, request.getPayeeFsp(), connectorId);

                callback.putTransfers(
                    config.getTransfersUrl(), msg.getCorrelationId(), connectorId,
                    msg.getPayerFsp(), transferId, abortedResponse(request));
                return;
            }

            IlpService.ParsedPrepare prepare = ilp.unwrap(request.getIlpPacket());
            IlpAgreement agreement = ilp.parseAgreement(prepare, IlpAgreement.class);

            if (agreement == null || agreement.payee() == null) {
                throw new IllegalStateException(
                    "ILP agreement payee is missing for transferId=" + transferId);
            }

            String payeeMobile = agreement.payee().getPartyIdentifier();
            if (payeeMobile == null || payeeMobile.isBlank()) {
                throw new IllegalStateException(
                    "Payee partyIdentifier missing from ILP agreement for transferId=" +
                        transferId);
            }

            long amountMinor = ilp.toMinorUnits(amount, currency);
            long lifetimeSeconds = ilp.resolveLifetimeSeconds(request.getExpiration());

            IlpService.FulfilResult fulfilResult = ilp.computeFulfilment(
                connectorId, amountMinor,
                prepare.data(), request.getCondition(), lifetimeSeconds);

            if (!fulfilResult.valid()) {
                throw new IllegalStateException(
                    "ILP condition mismatch for transferId=" + transferId);
            }

            Money payeeReceiveAmount = agreement.payeeReceiveAmount();

            String homeTransactionId = reserveTransfer(
                transferId, payeeMobile, amount, payeeReceiveAmount);

            pendingStore.set(
                transferId,
                new PendingTransfer(payeeMobile, amount, payeeReceiveAmount,currency, homeTransactionId,request.getExtensionList()));

            TransfersIDPutResponse response = new TransfersIDPutResponse()
                                                  .transferState(TransferState.RESERVED)
                                                  .fulfilment(fulfilResult.base64Fulfillment())
                                                  .completedTimestamp(now())
                                                  .extensionList(request.getExtensionList());

            LOG.info(
                "Post transfer response from Payee cc to Hub for TransferId {} : {}",
                request.getTransferId(), this.objectMapper.writeValueAsString(response));

            callback.putTransfers(
                config.getTransfersUrl(), msg.getCorrelationId(), connectorId, msg.getPayerFsp(),
                transferId, response);

            LOG.info(
                "postTransfers RESERVED transferId={} payeeMobile={} homeTransactionId={}",
                transferId, payeeMobile, homeTransactionId);
        } catch (Exception err) {
            LOG.error("postTransfers failed transferId={}: {}", transferId, err.getMessage(), err);

            if (transferId != null) {
                pendingStore.delete(transferId);

                callback.putTransfersError(
                    config.getTransfersUrl(), msg.getCorrelationId(), connectorId,
                    msg.getPayerFsp(), transferId, toErrorResponse(err, transferId));
            }
        }
    }

    private String reserveTransfer(String transferId,
                                   String payeeMobile,
                                   String amount,
                                   Money payeeReceiveAmount)
        throws JsonProcessingException, PostTransferException {

        ReservationForTransfer.Request request = new ReservationForTransfer.Request();
        request.setTransferId(transferId);
        request.setAmount(amount);

        FspParty payee = new FspParty();
        payee.setIdValue(payeeMobile);
        request.setTo(payee);

        ReservationForTransfer.Quote quote = new ReservationForTransfer.Quote();
        if (payeeReceiveAmount != null && payeeReceiveAmount.getAmount() != null &&
                !payeeReceiveAmount.getAmount().isBlank()) {
            quote.setPayeeReceiveAmount(new BigDecimal(payeeReceiveAmount.getAmount()));
        }
        request.setQuote(quote);

        ReservationForTransfer.Response response = fspClientService.doReservationForTransfer(
            request);

        if (response == null) {
            throw new PostTransferException(
                String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode()),
                "No response from DFSP backend for transferId=" + transferId);
        }

        if (response.getError() != null && response.getError().getErrorInformation() != null) {
            var errorInformation = response.getError().getErrorInformation();
            throw new PostTransferException(
                errorInformation.getStatusCode(), errorInformation.getMessage());
        }

        LOG.info(
            "Post transfer response from Payee for TransferId {} : {}", request.getTransferId(),
            this.objectMapper.writeValueAsString(response));

        return response.getHomeTransactionId();
    }

    private TransfersIDPutResponse abortedResponse(TransfersPostRequest request) {

        return new TransfersIDPutResponse()
                   .transferState(TransferState.ABORTED)
                   .completedTimestamp(now())
                   .extensionList(request.getExtensionList());
    }

    private ErrorInformationResponse toErrorResponse(Exception err, String idValue)
        throws JsonProcessingException {

        String errorCode = String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode());
        String errorDescription = err.getMessage();
        if (err instanceof TransfersListener.PostTransferException postTransferException) {
            errorCode = postTransferException.errorCode();
            errorDescription = postTransferException.getMessage();
        }
        ErrorInformation errorInformation = new ErrorInformation()
                                                .errorCode(errorCode)
                                                .errorDescription(errorDescription);
        ErrorInformationResponse errorInformationResponse = new ErrorInformationResponse().errorInformation(
            errorInformation);
        LOG.error(
            "Post Transfer error Response from Payee cc to HUB for idValue {} : {}", idValue,
            this.objectMapper.writeValueAsString(errorInformationResponse));

        return errorInformationResponse;
    }

    private static final class PostTransferException extends Exception {

        private final String errorCode;

        private PostTransferException(String errorCode, String message) {

            super(message);
            this.errorCode = errorCode;
        }

        private String errorCode() {

            return errorCode;
        }

    }

    private String now() {

        return java.time.format.DateTimeFormatter
                   .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                   .withZone(java.time.ZoneOffset.UTC)
                   .format(java.time.Instant.now());
    }

}
