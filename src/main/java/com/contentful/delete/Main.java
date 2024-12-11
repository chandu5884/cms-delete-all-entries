package com.contentful.delete;

import com.contentful.java.cda.CDAArray;
import com.contentful.java.cda.CDAClient;
import com.contentful.java.cda.CDAEntry;
import com.contentful.java.cma.CMAClient;
import com.contentful.java.cma.model.CMAEntry;

import java.util.List;
import java.util.stream.Collectors;

public class Main {

  public static void main(String[] args) {
    String spaceId = "Space Id here";
    String environmentId = "env id here";
    CMAClient cmaClient = new CMAClient.Builder()
        .setAccessToken("Set CMA Token Here")
        .setSpaceId(spaceId)
        .setEnvironmentId(environmentId)
        .build();
    CDAClient cdaClient = CDAClient.builder().setToken("CDA Token here").setSpace(spaceId).setEnvironment(environmentId).build();


    deleteEntries(cdaClient, cmaClient);
  }

  private static void deleteEntries(CDAClient cdaClient, CMAClient cmaClient) {

    int skip = 0;
    int totalEntries = Integer.MAX_VALUE;

    while (skip < totalEntries) {
      // Fetch a page of entries
      CDAArray cdaArrayPage = cdaClient
          .fetch(CDAEntry.class)
          .include(1)
          .limit(100)
          .skip(skip)
          .all();

      if (totalEntries == Integer.MAX_VALUE) {
        totalEntries = cdaArrayPage.total();
        System.out.println("Total entries to process: " + totalEntries);
      }

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

      skip += 100;
    }

    System.out.println("All entries processed.");
  }
}
