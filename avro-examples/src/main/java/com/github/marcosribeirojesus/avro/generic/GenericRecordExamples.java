package com.github.marcosribeirojesus.avro.generic;

import java.io.IOException;
import java.io.File;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordExamples {

  public static void main(String[] arg) {

    // step 0: define a schema
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" + "     \"type\": \"record\",\n" + "     \"namespace\": \"com.example\",\n"
        + "     \"name\": \"Customer\",\n" + "     \"fields\": [\n"
        + "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n"
        + "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n"
        + "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n"
        + "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n"
        + "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n"
        + "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n"
        + "     ]\n" + "}");

    // step 1: create a generic record
    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "Marcos");
    customerBuilder.set("last_name", "Ribeiro");
    customerBuilder.set("age", 37);
    customerBuilder.set("height", 185f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    GenericData.Record customer = customerBuilder.build();
    System.out.println(customer);

    GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
    customerBuilderWithDefault.set("first_name", "John");
    customerBuilderWithDefault.set("last_name", "Doe");
    customerBuilderWithDefault.set("age", 26);
    customerBuilderWithDefault.set("height", 175f);
    customerBuilderWithDefault.set("weight", 70.5f);
    GenericData.Record customerWithDefault = customerBuilderWithDefault.build();
    System.out.println(customerWithDefault);

    // step 2: write that generic record to a file
    final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
      dataFileWriter.append(customer);
      System.out.println("Written customer-generic.avro");
      dataFileWriter.close();
    } catch (IOException e) {
      System.out.println("Couldn't write file");
      e.printStackTrace();
    }

    // step 3: read a generic record from a file
    final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));

            // read a non existent field
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        }
        catch(IOException e) {
            e.printStackTrace();
        }

    // step 4: interpret as a generic record
  }

}
