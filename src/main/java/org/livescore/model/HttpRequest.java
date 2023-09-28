package org.livescore.model;

public record HttpRequest(String cacheFillBytes,
                          boolean cacheHit,
                          boolean cacheLookup,
                          String latency,
                          String referer,
                          String remoteIp,
                          String requestMethod,
                          String requestSize,
                          String requestUrl,
                          String responseSize,
                          int status,
                          String userAgent) {
}
