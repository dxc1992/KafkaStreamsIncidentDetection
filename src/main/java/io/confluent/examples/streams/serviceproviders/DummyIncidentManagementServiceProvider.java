package io.confluent.examples.streams.serviceproviders;

import io.confluent.examples.streams.AnomalyDetection;
import io.confluent.examples.streams.message.AgentMessage;

import java.net.http.HttpResponse;

public class DummyIncidentManagementServiceProvider implements IncidentManagementService<AnomalyDetection.IncidentMessage, String>{

    @Override
    public String raiseIncident(AnomalyDetection.IncidentMessage agentMessage) {
        return "IncidentRaised";
    }

    @Override
    public String findDuplicateIncident(AnomalyDetection.IncidentMessage agentMessage) {
        return "No Duplicates";
    }

    @Override
    public Boolean checkDuplicateIncident(AnomalyDetection.IncidentMessage incidentMessage) {
        return false;
    }
}
