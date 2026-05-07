package com.example.transforms;

import com.example.Options;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.mondodb.MongoDbIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class Fetch extends PTransform<PBegin, PCollection<Entity>> {
  private static final String DATABASE = "banana-pancakes";
  private static final String COLLECTION = "bubble-toes";

  private String user = "";
  private String password = "";

  public Fetch(Options options) {}

  public PCollection<Entity> extends(PBegin input) {
    return input
        .apply(MongoDbIO
            .read()
            .withUri(getUri())
            .withDatabase(DATABASE)
            .withCollection(COLLECTION))
        .apply(ParDo.of(new ToPojoFn()))
        .setCoder(SerializableCoder.of(Entity.class));
  }

  private String getUri() {
    return "mongodb+srv://" + user + ":" + password + "@banana-pancakes.hnbxk0t.mongodb.net/";
  }

  private class ToPojoFn extends DoFn<Document, Entity> {
    @ProcessElment
    public void processElement(@Element Document document, OutputReceiver<Entity> receiver) {
      Entity dto = new Entity();
      dto.setField1(document.getString("field1"));
      dto.setField2(document.getString("field2"));
      dto.setField3(document.getString("field3"));
      receiver.output(dto);
    }
  }
}
