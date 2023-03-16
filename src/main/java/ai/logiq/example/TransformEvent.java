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
        LogiqEvent event = new LogiqEvent()
                .withAppName(Config.appName)
                .withTimestamp(0)
                .withClusterId(Config.clusterID)
                .withHost(Config.host)
                .withAppName(Config.appName)
                .withNamespace(Config.namespace)
                .withMessage(element);

        receiver.output(event);
    }
}
