package com.example.qdranthackathon;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.langchainbeam.model.EmbeddingOutput;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import io.qdrant.client.grpc.Points.PointStruct;

public class QdrantFn extends DoFn<EmbeddingOutput, Void> {

    private final String host;
    private final String apiKey;
    private transient QdrantClient client;

    public QdrantFn(String host, String apiKey) {
        this.host = host;
        this.apiKey = apiKey;
    }

    @Setup
    public void setup() {

        this.client = new QdrantClient(
                QdrantGrpcClient.newBuilder(
                        this.host,
                        6334,
                        true)
                        .withApiKey(
                                this.apiKey)
                        .build());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        EmbeddingOutput element = c.element();

        client
                .upsertAsync(
                        "star_charts",
                        List.of(
                                PointStruct.newBuilder()
                                        .setVectors(vectors(element.getEmbedding().vector()))
                                        .putAllPayload(Map.of("document", value(
                                                element.getInputElement())))
                                        .build()));

    }
}
