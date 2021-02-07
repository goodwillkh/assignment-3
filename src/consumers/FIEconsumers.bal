import ballerina/io;
import ballerina/http;
import ballerinax/kafka;
import ballerina/log;
import ballerina/docker;

@docker:Config{}


kafka:ConsumerConfiguration consumerConfiguration = {

    bootstrapServers: "localhost:9092",

    groupId: "fie-group",
    offsetReset: "earliest",

    topics: ["hodAssignFie"]

};

kafka:Consumer consumer = checkpanic new (consumerConfiguration);
http:Client clientEndpoint = check new ("http://localhost:9050");

map<json> assignedProposals = {};

public function main() {
    while(true){
        io:println("*********FIE*********");
        //get list of proposals and evaluation them

        extractProposal("hodAssignFie");
        io:println(assignedProposals);

        string applicant = io:readln("studentNumber: ");
        string approved = io:readln("approved: ");

        var  response = clientEndpoint->post("/graphql",{ query: " { proposalSanction(studentNumber: \""+ applicant +"\", approved: \"" + approved +"\") }" });
        if (response is  http:Response) {
            var jsonResponse = response.getJsonPayload();

            if (jsonResponse is json) {
                
                io:println(jsonResponse);
            } else {
                io:println("Invalid payload received:", jsonResponse.message());
            }

        }
    }
}

function extractProposal(string topic){
    kafka:ConsumerRecord[] records = checkpanic consumer->poll(1000);

    foreach var kafkaRecord in records {
        if(kafkaRecord.offset.partition.topic == topic){
            byte[] messageContent = kafkaRecord.value;
            string|error message = string:fromBytes(messageContent);

            if (message is string) {
                json|error jsonContent = message.fromJsonString();

                if(jsonContent is json){
                    json|error stN = jsonContent.studentNumber;
                    json|error fie = jsonContent.fieID;


                    if(stN is json && fie is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error fieID = fie.toString();

                        if(studentNumber is int && fieID is string ){
                            assignedProposals[studentNumber.toString()] = {studentNumber, fieID};
                        }
                    }
                    
                }

            } else {
                log:printError("Error occurred while converting message data",
                    err = message);
            }
        }
    }
}
