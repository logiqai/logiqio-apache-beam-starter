package ai.logiq.example;

import logiqio.LogiqEvent;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.Instant;

class Config {
    public static String namespace = "ns";
    public static String host = "test-env";
    public static String appName = "test-app";
    public static String clusterID = "test-cluster";
}

public class TransformEvent extends DoFn<String, LogiqEvent> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<LogiqEvent> receiver) {
        LogiqEvent event = new LogiqEvent(Config.namespace, element, (int) Instant.now().getEpochSecond(), Config.host, String.valueOf(element.length()), Config.appName, Config.clusterID);
        receiver.output(event);
    }
}
