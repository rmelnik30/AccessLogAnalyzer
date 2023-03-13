package org.livescore;

import nl.basjes.parse.core.Field;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

public class LogEntry {
    public long timestamp;
    public String uri;
    public long processingTime;

    @Field("TIME.DATE:request.receive.time.date_utc")
    public void setDate(String date) throws ParseException {
        this.timestamp += DateUtils.parseDate(date, "yyyy-MM-dd").getTime();
    }

    @Field("TIME.TIME:request.receive.time.time_utc")
    public void setTime(String time) throws ParseException {
        this.timestamp += DateUtils.parseDate(time, "HH:mm:ss").getTime();
    }

    @Field("HTTP.URI:request.firstline.uri")
    public void setUri(String uri) {
        this.uri = uri;
    }

    @Field("MILLISECONDS:response.server.processing.time")
    public void setProcessingTime(String value) {
        this.processingTime = Long.parseLong(value);
    }

}
