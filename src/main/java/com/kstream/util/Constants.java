package com.kstream.util;

public interface Constants {

	static final String PROPERTIES_FILE = "custom.properties";

	final static String FINAL_SCHEMA = "{\"name\": \"CustomerPolicyClaimPaymentList\",\"type\": \"record\",\"fields\": [{\"name\": \"PolicyList\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"Data\",\"type\": \"record\",\"fields\": [{\"name\": \"policyendtime\",\"type\": \"double\"}, {\"name\": \"policystarttime\",\"type\": \"double\"}, {\"name\": \"pvar0\",\"type\": \"double\"}, {\"name\": \"policy\",\"type\": \"long\"}, {\"name\": \"pvar1\",\"type\": \"double\"}]}}}, {\"name\": \"CustomerList\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"customerData\",\"type\": \"record\",\"fields\": [{\"name\": \"customer\",\"type\": \"string\"}, {\"name\": \"customertime\",\"type\": \"double\"}, {\"name\": \"address\",\"type\": \"string\"}]}}},{\"name\": \"ClaimList\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"ClaimData\",\"type\": \"record\",\"fields\": [{\"name\": \"claimnumber\",	\"type\": \"string\"}, {	\"name\": \"claimreporttime\",	\"type\": \"double\"}, {\"name\": \"claimcounter\",\"type\": \"double\"}, {\"name\": \"claimtime\",\"type\": \"double\"}]}}},{\"name\": \"PaymentList\",\"type\": {\"type\": \"array\",\"items\": {\"name\": \"PaymentData\",\"type\": \"record\",\"fields\": [{\"name\": \"claimnumber\",\"type\": \"string\"}, {\"name\": \"paytime\",\"type\": \"double\"}, {\"name\": \"claimcounter\",\"type\": \"double\"}, {\"name\": \"payment\",	\"type\": \"double\"	}]}}}]}";

	final static String OUTPUT_SCHEMA1 = "{\"name\":\"CustomerPolicyClaimPaymentList\",\"type\":\"record\",\"fields\":[{\"name\":\"policyRecords\",\"type\":{\"type\":\"array\",\"default\": [],\"items\":{\"name\":\"Data\",\"type\":\"record\",\"fields\":[{\"name\":\"policyendtime\",\"type\":\"string\"},{\"name\":\"policystarttime\",\"type\":\"string\"},{\"name\":\"pvar0\",\"type\":\"string\"},{\"name\":\"policy\",\"type\":\"string\"},{\"name\":\"pvar1\",\"type\":\"string\"}]}}},{\"name\":\"customerRecords\",\"type\":{\"type\":\"array\",\"default\": [], \"items\":{\"name\":\"customerData\",\"type\":\"record\",\"fields\":[{\"name\":\"customer\",\"type\":\"string\"},{\"name\":\"customertime\",\"type\":\"string\"},{\"name\":\"address\",\"type\":\"string\"}]}}}]}";

	static final String PIPELINE_PROPERTY_FILE = "templates/template.properties";

	static final String CUSTOM_PROPERTY_FILE = "custom.properties";
	
	
	static final String STRING_SERDE = "StringSerde";

}
