package io.confluent.examples.streams.serviceproviders;

public interface IncidentManagementService<REQUEST, RESPONSE> {

    RESPONSE raiseIncident(REQUEST request);

    RESPONSE findDuplicateIncident(REQUEST request);

    Boolean checkDuplicateIncident(REQUEST request);
}
