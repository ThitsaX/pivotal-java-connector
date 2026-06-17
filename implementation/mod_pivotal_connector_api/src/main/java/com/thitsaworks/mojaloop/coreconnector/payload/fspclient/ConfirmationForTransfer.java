package com.thitsaworks.mojaloop.coreconnector.payload.fspclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.ErrorInformationResponse;
import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.StateEnum;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Extension;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ExtensionList;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.GeoCode;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Money;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Party;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.PartyIdType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public class ConfirmationForTransfer {

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Request implements Serializable {

        @JsonProperty("transferId")
        private String transferId;

        @JsonProperty("homeTransactionId")
        private String homeTransactionId;

        @JsonProperty("direction")
        private String direction;

        @JsonProperty("quote")
        private Quote quote;

        @JsonProperty("quoteRequest")
        private QuoteRequest quoteRequest;

        @JsonProperty("fulfil")
        private String fulfil;

        @JsonProperty("currentState")
        private StateEnum currentState;

        @JsonProperty("initiatedTimestamp")
        private String initiatedTimestamp;

        @JsonProperty("finalNotification")
        private FinalNotification finalNotification;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QuoteRequest {

        @JsonProperty("headers")
        private Object headers;

        @JsonProperty("body")
        private Body body;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Prepare {

        @JsonProperty("headers")
        private Object headers;

        @JsonProperty("body")
        private PrepareBody body;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PrepareBody {

        @JsonProperty("transferId")
        private String transferId;

        @JsonProperty("payeeFspId")
        private String payeeFsp;

        @JsonProperty("payerFspId")
        private String payerFsp;

        @JsonProperty("amount")
        private Money amount;

        @JsonProperty("ilpPacket")
        private String ilpPacket;

        @JsonProperty("condition")
        private String condition;

        @JsonProperty("expiration")
        private String expiration;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InternalRequest {

        @JsonProperty("quoteId")
        private String quoteId;

        @JsonProperty("transactionId")
        private String transactionId;

        @JsonProperty("to")
        private To to;

        @JsonProperty("from")
        private From from;

        @JsonProperty("amountType")
        private String amountType;

        @JsonProperty("amount")
        private BigDecimal amount;

        @JsonProperty("currency")
        private String currency;

        @JsonProperty("feesAmount")
        private BigDecimal feesAmount;

        @JsonProperty("feesCurrency")
        private String feesCurrency;

        @JsonProperty("transactionType")
        private String transactionType;

        @JsonProperty("initiator")
        private String initiator;

        @JsonProperty("initiatorType")
        private String initiatorType;

        @JsonProperty("geoCode")
        private GeoCode geoCode;

        @JsonProperty("note")
        private String note;

        @JsonProperty("expiration")
        private String expiration;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Fulfil {

        @JsonProperty("headers")
        private Object headers;

        @JsonProperty("body")
        private String body;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FinalNotification {

        @JsonProperty("completedTimestamp")
        private String completedTimestamp;

        @JsonProperty("transferState")
        private String transferState;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Quote {

        @JsonProperty("request")
        private Body request;

        @JsonProperty("internalRequest")
        private InternalRequest internalRequest;

        @JsonProperty("response")
        private InternalResponse response;

        @JsonProperty("mojaloopResponse")
        private MoJaLoopResponse mojaloopResponse;

        @JsonProperty("fulfilment")
        private String fulfilment;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class From {

        @JsonProperty("id_type")
        private PartyIdType idType;

        @JsonProperty("id_value")
        private String idValue;

        @JsonProperty("fsp_id")
        private String fspId;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class To {

        @JsonProperty("id_type")
        private PartyIdType idType;

        @JsonProperty("id_value")
        private String idValue;

        @JsonProperty("id_sub_value")
        private String idSubValue;

        @JsonProperty("fsp_id")
        private String fspId;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

        @JsonProperty("last_name")
        private String lastName;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Fees {

        @JsonProperty("currency")
        private String currency;

        @JsonProperty("amount")
        private BigDecimal amount;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransactionType {

        @JsonProperty("scenario")
        private String scenario;

        @JsonProperty("subScenario")
        private String subScenario;

        @JsonProperty("initiator")
        private String initiator;

        @JsonProperty("initiatorType")
        private String initiatorType;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Body {

        @JsonProperty("quoteId")
        private String quoteId;

        @JsonProperty("transactionId")
        private String transactionId;

        @JsonProperty("amountType")
        private String amountType;

        @JsonProperty("amount")
        private Money amount;

        @JsonProperty("payeeReceiveAmount")
        private BigDecimal payeeReceiveAmount;

        @JsonProperty("fees")
        private Fees fees;

        @JsonProperty("expiration")
        private String expiration;

        @JsonProperty("payer")
        private Party payer;

        @JsonProperty("payee")
        private Party payee;

        @JsonProperty("transactionType")
        private TransactionType transactionType;

        @JsonProperty("note")
        private String note;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Response implements Serializable {

        @JsonProperty("home_transaction_id")
        private String homeTransactionId;

        @JsonProperty("error")
        private ErrorInformationResponse.ErrorInformation error;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QuoteResponse {

        @JsonProperty("headers")
        private Object headers;

        @JsonProperty("body")
        private String body;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InternalResponse {

        @JsonProperty("quoteId")
        private String quoteId;

        @JsonProperty("transactionId")
        private String transactionId;

        @JsonProperty("transferAmount")
        private BigDecimal transferAmount;

        @JsonProperty("transferAmountCurrency")
        private String transferAmountCurrency;

        @JsonProperty("payeeReceiveAmount")
        private BigDecimal payeeReceiveAmount;

        @JsonProperty("payeeReceiveAmountCurrency")
        private String payeeReceiveAmountCurrency;

        @JsonProperty("payeeFspFeeAmount")
        private BigDecimal payeeFspFeeAmount;

        @JsonProperty("payeeFspFeeAmountCurrency")
        private String payeeFspFeeAmountCurrency;

        @JsonProperty("payeeFspCommissionAmount")
        private BigDecimal payeeFspCommissionAmount;

        @JsonProperty("payeeFspCommissionAmountCurrency")
        private String payeeFspCommissionAmountCurrency;

        @JsonProperty("expiration")
        private String expiration;

        @JsonProperty("geoCode")
        private GeoCode geoCode;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MoJaLoopResponse {

        private Money transferAmount;

        private String expiration;

        private String ilpPacket;

        private String condition;

        private Money payeeReceiveAmount;

        private Money payeeFspFee;

        private Money payeeFspCommission;

        private GeoCode geoCode;

        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ErrorInformation {

        @JsonProperty("errorCode")
        private String errorCode;

        @JsonProperty("errorDescription")
        private String errorDescription;

        @JsonProperty("extensionList")
        private ExtensionList extensionList;

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MoJaLoopError {

        @JsonProperty("errorInformation")
        private ErrorInformation errorInformation;

    }

}
