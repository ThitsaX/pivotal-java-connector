package com.thitsaworks.mojaloop.coreconnector.mapper.nats;

import com.thitsaworks.mojaloop.coreconnector.component.mojaloop.FspParty;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.TransfersPostRequest;
import com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store.IlpAgreement;
import com.thitsaworks.mojaloop.coreconnector.payload.fspclient.ReservationForTransfer;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
@Component
public class PostTransferMapper {

    public ReservationForTransfer.Request transferMapper(IlpAgreement agreement, TransfersPostRequest transfersPostRequest){
        FspParty payer = new FspParty();
        payer.setIdType(agreement.payer().getPartyIdType());
        payer.setIdValue(agreement.payer().getPartyIdentifier());
        payer.setIdSubValue(agreement.payer().getPartySubIdOrType() == null ? "" :
                                agreement.payer().getPartySubIdOrType());
        payer.setFspId(agreement.payer().getFspId());

        FspParty payee = new FspParty();
        payee.setIdType(agreement.payee().getPartyIdType());
        payee.setIdValue(agreement.payee().getPartyIdentifier());
        payee.setIdSubValue(agreement.payee().getPartySubIdOrType() == null ? "" :
                                agreement.payee().getPartySubIdOrType());
        payee.setFspId(agreement.payee().getFspId());

        ReservationForTransfer.Request request = new ReservationForTransfer.Request();
        request.setAmount(transfersPostRequest.getAmount().getAmount());
        request.setAmountType(agreement.amountType());
        request.setCurrency(transfersPostRequest.getAmount().getCurrency());
        request.setFrom(payer);
        request.setNote(agreement.note());
        request.setTransferId(request.getTransferId());

        ReservationForTransfer.Quote quote = new ReservationForTransfer.Quote();
        if (transfersPostRequest.getAmount().getAmount() != null &&
                !transfersPostRequest.getAmount().getAmount().isBlank()) {
            quote.setPayeeReceiveAmount(new BigDecimal(agreement.payeeReceiveAmount().getAmount()));
            quote.setTransferAmount(new BigDecimal(agreement.transferAmount().getAmount()));
            quote.setExtensionList(agreement.extensionList());
        }
        request.setQuote(quote);
        request.setQuoteRequestExtensions(agreement.extensionList());
        request.setSubScenario(agreement.subScenario());
        request.setTo(payee);
        request.setTransferId(transfersPostRequest.getTransferId());
        return  request;

    }
}


