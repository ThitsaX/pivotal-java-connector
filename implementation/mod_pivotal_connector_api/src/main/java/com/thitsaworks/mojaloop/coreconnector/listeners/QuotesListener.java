package com.thitsaworks.mojaloop.coreconnector.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thitsaworks.mojaloop.coreconnector.CoreConnectorConfiguration;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.ErrorCode;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformation;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.QuotesIDPutResponse;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.QuotesPostRequest;
import com.thitsaworks.mojaloop.coreconnector.ilp.IlpService;
import com.thitsaworks.mojaloop.coreconnector.logging.MdcExtractors;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsPullListener;
import com.thitsaworks.mojaloop.coreconnector.mapper.nats.PostQuoteMapper;
import com.thitsaworks.mojaloop.coreconnector.nats.NatsService;
import com.thitsaworks.mojaloop.coreconnector.payload.fspclient.DoQuote;
import com.thitsaworks.mojaloop.coreconnector.payload.nats.PostQuotesNatsMessage;
import com.thitsaworks.mojaloop.coreconnector.services.FspClientService;
import io.nats.client.JetStreamManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class QuotesListener implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(QuotesListener.class);

    private final NatsService natsService;

    private final FspiopCallbackService callback;

    private final IlpService ilp;

    private final FspClientService fspClientService;

    private final CoreConnectorConfiguration.Settings config;

    private final PostQuoteMapper quotesResponseMapper;

    private final ObjectMapper objectMapper;

    private NatsPullListener<PostQuotesNatsMessage> listener;

    public QuotesListener(NatsService natsService,
                          FspiopCallbackService callback,
                          IlpService ilp,
                          FspClientService fspClientService,
                          CoreConnectorConfiguration.Settings config,
                          PostQuoteMapper quotesResponseMapper,
                          ObjectMapper objectMapper) {

        this.natsService = natsService;
        this.callback = callback;
        this.ilp = ilp;
        this.fspClientService = fspClientService;
        this.config = config;
        this.quotesResponseMapper = quotesResponseMapper;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        String connectorId = config.getConnectorId();
        String subject = natsService.getQuotesSubject();
        String durable = natsService.normalizeDurable(connectorId, "connector-consumer-post-quotes");

        JetStreamManagement jsm = natsService.jetstreamManager();
        String stream = natsService.ensureStream(jsm, subject);
        natsService.ensureConsumer(jsm, stream, durable, subject);

        LOG.info("Listening on '{}'", subject);

        listener = new NatsPullListener<>(LOG,
                                          natsService,
                                          "PostQuotes",
                                          PostQuotesNatsMessage.class,
                                          data -> handle(data, connectorId),
                                          MdcExtractors::postQuotes);
        listener.start(subject, stream, durable, "quotes-listener");
    }

    @Override
    public void destroy() {

        if (listener != null) {
            listener.stop();
        }
    }

    private void handle(PostQuotesNatsMessage msg, String connectorId) throws Exception {

        QuotesPostRequest request = msg.getRequest();
        String quoteId = request != null ? request.getQuoteId() : null;
        LOG.info("Post Quote request info from the Inbound to Payee cc for TransferId {} : {}",
                 request.getTransactionId(),
                 this.objectMapper.writeValueAsString(request));

        try {
            if (request == null) {
                throw new IllegalArgumentException("Quotes request is missing");
            }

            String
                amount =
                request.getAmount()
                       .getAmount();
            String
                currency =
                request.getAmount()
                       .getCurrency()
                       .toString();

            LOG.info("postQuotes quoteId={} amount={} {}", quoteId, amount, currency);

            DoQuote.Request quoteRequestToFsp = quotesResponseMapper.toPostQuoteRequest(request);
            DoQuote.Response responseFromFsp = fspClientService.doQuote(quoteRequestToFsp);

            if (responseFromFsp == null) {
                throw new PostQuoteException(String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode()),
                                             "No response from DFSP backend for quoteId=" + quoteId);
            }

            if (responseFromFsp.getError() != null && responseFromFsp.getError()
                                                                     .getErrorInformation() != null) {
                var
                    errorInformation =
                    responseFromFsp.getError()
                                   .getErrorInformation();
                throw new PostQuoteException(errorInformation.getStatusCode(), errorInformation.getMessage());
            }

            LOG.info("Quote response from Payee for TransferId {} : {}",
                     request.getTransactionId(),
                     this.objectMapper.writeValueAsString(responseFromFsp));

            IlpService.Expiration expiration = ilp.buildExpiration();

            var agreement = quotesResponseMapper.toIlpAgreement(request, responseFromFsp, expiration.epochMs());
            long amountMinor = ilp.toMinorUnits(responseFromFsp.getTransferAmount(),
                                                responseFromFsp.getTransferAmountCurrency()
                                                               .toString());

            IlpService.PrepareResult ilpResult = ilp.prepare(connectorId, amountMinor, agreement, expiration.epochMs());

            QuotesIDPutResponse response = quotesResponseMapper.toPutQuoteResponse(request,
                                                                                   responseFromFsp,
                                                                                   expiration.isoString(),
                                                                                   ilpResult.base64PreparePacket(),
                                                                                   ilpResult.base64Condition());

            callback.putQuotes(config.getQuotesUrl(),
                               msg.getCorrelationId(),
                               connectorId,
                               msg.getPayerFsp(),
                               quoteId,
                               response);

            LOG.info("Quote Response from Payee cc to hub for TransferId {} : {}",
                     request.getTransactionId(),
                     this.objectMapper.writeValueAsString(response));
        } catch (Exception err) {
            LOG.error("postQuotes failed quoteId={}: {}", quoteId, err.getMessage(), err);
            if (quoteId != null) {
                callback.putQuotesError(config.getQuotesUrl(),
                                        msg.getCorrelationId(),
                                        connectorId,
                                        msg.getPayerFsp(),
                                        quoteId,
                                        toErrorResponse(err, quoteId));
            }
        }
    }

    private ErrorInformationResponse toErrorResponse(Exception err, String idValue) throws JsonProcessingException {

        String errorCode = String.valueOf(ErrorCode.GENERIC_DOWNSTREAM_ERROR_PAYEE.getStatusCode());
        String errorDescription = err.getMessage();

        if (err instanceof QuotesListener.PostQuoteException postQuoteException) {
            errorCode = postQuoteException.errorCode();
            errorDescription = postQuoteException.getMessage();
        }

        ErrorInformation
            errorInformation =
            new ErrorInformation().errorCode(errorCode)
                                  .errorDescription(errorDescription);
        ErrorInformationResponse errorInformationResponse = new ErrorInformationResponse().errorInformation(
            errorInformation);
        LOG.error("Post Quote error Response from Payee cc to HUB for idValue {} : {}",
                  idValue,
                  this.objectMapper.writeValueAsString(errorInformationResponse));

        return errorInformationResponse;
    }

    private static final class PostQuoteException extends Exception {

        private final String errorCode;

        private PostQuoteException(String errorCode, String message) {

            super(message);
            this.errorCode = errorCode;
        }

        private String errorCode() {

            return errorCode;
        }

    }

}
