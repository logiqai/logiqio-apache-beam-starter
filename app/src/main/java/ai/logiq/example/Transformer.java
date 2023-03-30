package ai.logiq.example;

import logiqio.LogiqEvent;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Transformer extends PTransform<PCollection<String>, PCollection<LogiqEvent>> {
    @Override
    public PCollection<LogiqEvent> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new TransformEvent()));
    }
}
