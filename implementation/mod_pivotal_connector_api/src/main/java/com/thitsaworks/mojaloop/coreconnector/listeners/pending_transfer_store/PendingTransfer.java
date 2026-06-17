package com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store;

import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ExtensionList;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Money;

public record PendingTransfer(String payeeMobile,
                              String payerMobile,
                              String amount,
                              Money payeeReceiveAmount,
                              String currency,
                              String homeTransactionId,
                              String payerFspId,
                              String payeeFspId,
                              String note,
                              ExtensionList extensionList) { }
