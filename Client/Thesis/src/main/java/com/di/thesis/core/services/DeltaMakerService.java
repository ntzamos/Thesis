package com.di.thesis.core.services;

public interface DeltaMakerService {

    boolean createDelta(String fileName, String hasHeader, String uniqueKeys,  String delimeter);
}
