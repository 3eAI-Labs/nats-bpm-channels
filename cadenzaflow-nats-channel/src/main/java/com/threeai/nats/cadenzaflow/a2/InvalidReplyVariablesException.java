package com.threeai.nats.cadenzaflow.a2;

/**
 * A reply's structured {@code variables}/{@code localVariables} object could not be converted to
 * engine variables. Checked deliberately: {@link A2ReplyPayloadDecoder} otherwise never throws,
 * and the one thing the completion path must not do with a conversion failure is proceed —
 * completing without the variables the worker asked for lets the process continue on wrong data.
 * {@link A2CompletionBridge} catches this and routes the reply to the DLQ as
 * {@code VAL_INVALID_REPLY_VARIABLES}.
 */
class InvalidReplyVariablesException extends Exception {

    InvalidReplyVariablesException(String message) {
        super(message);
    }

    InvalidReplyVariablesException(String message, Throwable cause) {
        super(message, cause);
    }
}
