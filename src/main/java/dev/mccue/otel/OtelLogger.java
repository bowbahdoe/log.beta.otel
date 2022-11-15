package dev.mccue.otel;

import dev.mccue.log.beta.Level;
import dev.mccue.log.beta.LogEntry;
import dev.mccue.log.beta.Logger;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.message.MapMessage;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class OtelLogger implements Logger {
    private final String namespace;
    private final org.apache.logging.log4j.Logger log4jLogger;

    public OtelLogger(String namespace) {
        this.namespace = namespace;
        this.log4jLogger = LogManager.getLogger(namespace);
    }

    @Override
    public void event(Level level, String name, List<LogEntry> list) {
        var log4jLevel = switch (level) {
            case TRACE -> org.apache.logging.log4j.Level.TRACE;
            case DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case INFO -> org.apache.logging.log4j.Level.INFO;
            case WARN -> org.apache.logging.log4j.Level.WARN;
            case ERROR -> org.apache.logging.log4j.Level.ERROR;
        };

        var mapMessage = new MapMessage<>(
                list.stream()
                        .collect(Collectors.toUnmodifiableMap(
                                LogEntry::key,
                                entry -> entry.value().toUnderlyingObject()
                        ))
        );

        log4jLogger.log(log4jLevel, mapMessage);
    }

    private void setSpanValue(Span span, String key, LogEntry.Value value) {
        if (value instanceof LogEntry.Value.Lazy lazy) {
            setSpanValue(span, key, lazy.value());
        }
        else if (value instanceof LogEntry.Value.Boolean b) {
            span.setAttribute(key, b.value());
        }
        else if (value instanceof LogEntry.Value.Byte b) {
            span.setAttribute(key, b.value());
        }
        else if (value instanceof LogEntry.Value.Short s) {
            span.setAttribute(key, s.value());
        }
        else if (value instanceof LogEntry.Value.Integer i) {
            span.setAttribute(key, i.value());
        }
        else if (value instanceof LogEntry.Value.Long l) {
            span.setAttribute(key, l.value());
        }
        else if (value instanceof LogEntry.Value.Float f) {
            span.setAttribute(key, f.value());
        }
        else if (value instanceof LogEntry.Value.Double d) {
            span.setAttribute(key, d.value());
        }
        else {
            span.setAttribute(key, Objects.toString(value.toUnderlyingObject()));
        }
    }

    @Override
    public <T> T span(Level level, String name, List<LogEntry> entries, Supplier<T> supplier) {
        var otel = GlobalOpenTelemetry.get();
        var tracer = otel.getTracer(namespace);
        var span = tracer.spanBuilder(name).startSpan();
        for (var entry : entries) {
            setSpanValue(span, entry.key(), entry.value());
        }

        try (var __ = span.makeCurrent()) {
            var result = supplier.get();
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Throwable t) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(t);
            throw t;
        } finally {
            span.end();
        }
    }

    @Override
    public <T> T withContext(List<LogEntry> list, Supplier<T> supplier) {
        if (list.size() == 0) {
            return supplier.get();
        }
        else {
            var first = list.get(0);
            var context = CloseableThreadContext.put(
                    first.key(),
                    Objects.toString(first.value().toUnderlyingObject())
            );
            for (int i = 1; i < list.size(); i++) {
                var entry = list.get(i);
                context.put(entry.key(), Objects.toString(entry.value().toUnderlyingObject()));
            }
            try (var __ = context) {
                return supplier.get();
            }
        }
    }
}
