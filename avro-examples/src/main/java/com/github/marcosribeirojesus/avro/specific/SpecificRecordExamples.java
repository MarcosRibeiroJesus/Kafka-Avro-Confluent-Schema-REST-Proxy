package com.github.marcosribeirojesus.avro.specific;

import java.io.File;
import java.io.IOException;

import com.example.Customer;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SpecificRecordExamples {

  /*
   * step 0: define a schema 
   * 0.1: delete target path 
   * 0.2: maven clean 
   * 0.3: maven package ~> it will generate the customer avro schema automatically from
   * resource by avro-maven-plugin
   **/

  public static void main(String[] args) {

    // step 1: create a specific record
    Customer.Builder customerBuilder = Customer.newBuilder();
    customerBuilder.setAge(30);
    customerBuilder.setFirstName("Marcos");
    customerBuilder.setLastName("Ribeiro");
    customerBuilder.setAutomatedEmail(true);
    customerBuilder.setHeight(180f);
    customerBuilder.setWeight(90f);

    Customer customer = customerBuilder.build();
    System.out.println(customer.toString());

    // step 2: write to a file
    final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

    try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
      dataFileWriter.append(customer);
      System.out.println("successfully wrote customer-specific.avro");
    } catch (IOException e) {
      e.printStackTrace();
    }

    // step 3: read from a file
    final File file = new File("customer-specific.avro");
    final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
    final DataFileReader<Customer> dataFileReader;
    try {
      System.out.println("Reading our specific record");
      dataFileReader = new DataFileReader<>(file, datumReader);
      while (dataFileReader.hasNext()) {
        Customer readCustomer = dataFileReader.next();

    // step 4: interpret
        System.out.println(readCustomer.toString());
        System.out.println("First name: " + readCustomer.getFirstName());
        System.out.println("Height: " + readCustomer.getHeight().toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }


  }
}
