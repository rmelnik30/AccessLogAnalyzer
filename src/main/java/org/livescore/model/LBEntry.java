package org.livescore.model;

import java.time.OffsetDateTime;

public record LBEntry(HttpRequest httpRequest, OffsetDateTime timestamp) {
}
