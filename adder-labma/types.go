package main

type KinesisPayload struct {
	Records []struct {
		AwsRegion         string `json:"awsRegion"`
		EventID           string `json:"eventID"`
		EventName         string `json:"eventName"`
		EventSource       string `json:"eventSource"`
		EventSourceARN    string `json:"eventSourceARN"`
		EventVersion      string `json:"eventVersion"`
		InvokeIdentityArn string `json:"invokeIdentityArn"`
		Kinesis           struct {
			Data                 string `json:"data"`
			KinesisSchemaVersion string `json:"kinesisSchemaVersion"`
			PartitionKey         string `json:"partitionKey"`
			SequenceNumber       string `json:"sequenceNumber"`
		} `json:"kinesis"`
	} `json:"Records"`
}
