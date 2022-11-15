package dev.mccue.otel;

import dev.mccue.log.beta.Logger;
import dev.mccue.log.beta.LoggerFactory;

public final class OtelLoggerFactory implements LoggerFactory {
    @Override
    public Logger createLogger(String namespace) {
        return new OtelLogger(namespace);
    }
}
