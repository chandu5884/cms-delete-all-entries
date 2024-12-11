package com.contentful.delete;

import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

  public static void main(String[] args) {
    Properties properties = new Properties();

    String cmaToken = "";
    String cdaToken = "";
    String spaceId = "";
    String environmentId = "";

    try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
      if (input == null) {
        System.out.println("Sorry, unable to find app.properties");
        return;
      }

      properties.load(input);

      cmaToken = properties.getProperty("cma.token");
      cdaToken = properties.getProperty("cda.token");
      spaceId = properties.getProperty("space.id");
      environmentId = properties.getProperty("environment.id");

    } catch (IOException e) {
      System.out.println("Error loading application properties");
    }

    CMAClient cmaClient = new CMAClient.Builder()
        .setAccessToken(cmaToken)
        .setSpaceId(spaceId)
        .setEnvironmentId(environmentId)
        .build();
    CDAClient cdaClient = CDAClient.builder().setToken(cdaToken).setSpace(spaceId).setEnvironment(environmentId).build();


    getEntries(cdaClient, cmaClient);
  }

  private static void getEntries(CDAClient cdaClient, CMAClient cmaClient) {

    int skip = 0;
    int batchSize = 100; // Define the limit for each fetch
    int totalEntries;

    do {
      // Fetch a page of entries
      CDAArray cdaArrayPage = cdaClient
          .fetch(CDAEntry.class)
          .include(1)
          .limit(batchSize)
          .skip(skip)
          .all();

      totalEntries = cdaArrayPage.total();
      System.out.println("Total entries to process: " + totalEntries);

      List<CDAEntry> entries = cdaArrayPage.items().stream()
          .filter(resource -> resource instanceof CDAEntry)
          .map(resource -> (CDAEntry) resource)
          .collect(Collectors.toList());

      for (CDAEntry cdaEntry : entries) {
        String entryId = cdaEntry.id();

        CMAEntry cmaEntry = cmaClient.entries().fetchOne(entryId);

        if (cmaEntry != null) {
          if (cmaEntry.isPublished()) {
            System.out.println("Unpublished entry with ID: " + entryId);
            cmaClient.entries().unPublish(cmaEntry);
          }
          System.out.println("Deleting entry with ID: " + entryId);
          cmaClient.entries().delete(cmaEntry);
        }
      }

      skip += entries.size(); // Increment by actual processed entries, not batch size

    } while (skip < totalEntries); // Continue until all entries are processed

    System.out.println("All entries processed.");
  }

}
