package com.example.qdranthackathon;

import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.langchainbeam.model.EmbeddingOutput;

import static io.qdrant.client.PointIdFactory.id;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.UpdateResult;

public class QdrantFn extends DoFn<EmbeddingOutput, Void> {

    private final String host;
    private final String apiKey;
    private final String collectionName;
    private transient QdrantClient client;

    public QdrantFn(String host, String apiKey, String collectionName) {
        this.host = host;
        this.apiKey = apiKey;
        this.collectionName = collectionName;
    }

    @Setup
    public void setup() {

        System.out.println("Qadrantfn setup");

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

        System.out.println("inserting element " + element.getInputElement());

        try {
            UpdateResult operationInfo = client
                    .upsertAsync(
                            this.collectionName,
                            List.of(
                                    PointStruct.newBuilder()
                                            .setId(id(new java.util.Random().nextLong()))
                                            .setVectors(vectors(element.getEmbedding().vector()))
                                            .putAllPayload(Map.of("document", value(
                                                    element.getInputElement())))
                                            .build()))
                    .get();

            System.out.println("insert status: " + operationInfo);
        } catch (Exception e) {
            System.out.println("error: " + e.getMessage());
        }

    }
}
