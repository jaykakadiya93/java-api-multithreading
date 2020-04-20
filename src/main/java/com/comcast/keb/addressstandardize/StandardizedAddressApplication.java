package com.comcast.keb.addressstandardize;


import com.fasterxml.jackson.core.JsonProcessingException;
import net.minidev.json.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class StandardizedAddressApplication implements CommandLineRunner {
    @Autowired
    StandardizedAddressRepository standardizedAddressRepository;
    @Autowired
    ReadSQS readSQS;

    public static void main(String[] args) throws JsonProcessingException, ParseException {

            ApplicationContext ctx =
                    SpringApplication.run(StandardizedAddressApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {
       while(true) {

           readSQS.getMessages();
           System.out.println("main method");
       }
    }
}