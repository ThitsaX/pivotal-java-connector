package com.thitsaworks.mojaloop.coreconnector.listeners.pending_transfer_store;

import com.thitsaworks.mojaloop.coreconnector.fspiop.model.ExtensionList;
import com.thitsaworks.mojaloop.coreconnector.fspiop.model.Money;

public record PendingTransfer(
    String payeeMobile,
    String amount,
    Money payeeReceiveAmount,
    String currency,
    String homeTransactionId,
    ExtensionList extensionList
) { }