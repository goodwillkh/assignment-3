import ballerina/io;
import ballerina/http;
import ballerinax/kafka;
import ballerina/log;

import ballerina/docker;

@docker:Config{}
kafka:ConsumerConfiguration consumerConfiguration = {

    bootstrapServers: "localhost:9092",

    groupId: "hod-group",
    offsetReset: "earliest",

    topics: ["supervisorApplicantSelection", "supervisorProposalReview", "supervisorThesisApproval", "hdcEvaluation"]

};

kafka:Consumer consumer = checkpanic new (consumerConfiguration);
http:Client clientEndpoint = check new ("http://localhost:9070");

map<json> supervisorInterests = {};
map<json> assignedProposals = {};

public function main() {
    while(true){
        io:println("*********HOD*********");

        io:println("1. Approve Supervisor Selection \n"
        + "2. Assign FIE \n"
        + "3. Final Submission \n"
        + "4. Assign FEE\n");

        string choice = io:readln("Enter choice 1 - 3: ");
        int c = checkpanic int:fromString(choice);

        if(c == 1){
            getMessages("supervisorApplicantSelection");
            io:println(supervisorInterests);

            string applicant = io:readln("studentNumber: ");
            string approved = io:readln("approved: ");
            var  response = clientEndpoint->post("/graphql",{ query: " { approveSupervisorSelection(studentNumber: "+ applicant +", approved: \""+ approved +"\") }" });
            if (response is  http:Response) {
                var jsonResponse = response.getJsonPayload();

                if (jsonResponse is json) {
                    
                    io:println(jsonResponse);
                } else {
                    io:println("Invalid payload received:", jsonResponse.message());
                }

            }
        }

        if(c == 2){
            extractProposal("supervisorProposalReview");
            io:println(assignedProposals);

            string applicant = io:readln("studentNumber: ");
            string fieID = io:readln("FIE: ");
            var  response = clientEndpoint->post("/graphql",{ query: " { assignFIE(studentNumber: "+ applicant +", fieID: "+ fieID +") }" });
            if (response is  http:Response) {
                var jsonResponse = response.getJsonPayload();

                if (jsonResponse is json) {
                    
                    io:println(jsonResponse);
                } else {
                    io:println("Invalid payload received:", jsonResponse.message());
                }

            }
        }

        if(c == 3){
        extractFinal("hdcEvaluation");
        io:println(assignedProposals);

            string applicant = io:readln("studentNumber: ");
            var  response = clientEndpoint->post("/graphql",{ query: " { finalSubmission(studentNumber: "+ applicant +") }" });
            if (response is  http:Response) {
                var jsonResponse = response.getJsonPayload();

                if (jsonResponse is json) {
                    
                    io:println(jsonResponse);
                } else {
                    io:println("Invalid payload received:", jsonResponse.message());
                }

            }
        }

        if(c == 4){

            string applicant = io:readln("studentNumber: ");
            string fieID = io:readln("FEE: ");
            string approved = io:readln("approved: ");

            var  response = clientEndpoint->post("/graphql",{ query: " { approveSupervisorSelection(studentNumber: "+ applicant +", approved: \""+ approved +"\") }" });
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
}



function getMessages(string topic){
    kafka:ConsumerRecord[] records = checkpanic consumer->poll(1000);

    foreach var kafkaRecord in records {
        if(kafkaRecord.offset.partition.topic == topic){
            byte[] messageContent = kafkaRecord.value;
            string|error message = string:fromBytes(messageContent);

            if (message is string) {
                json|error jsonContent = message.fromJsonString();

                if(jsonContent is json){
                    json|error stN = jsonContent.studentNumber;
                    json|error spID = jsonContent.supervisorID;

                    if(stN is json && spID is json ){
                        int|error studentNumber = int:fromString(stN.toString());
                        int|error supervisorID = int:fromString(spID.toString());


                        if(studentNumber is int && supervisorID is int){
                            supervisorInterests[studentNumber.toString()] = {studentNumber, supervisorID};
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
                    json|error prop = jsonContent.proposalApproved;


                    if(stN is json && prop is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error proposalApproved = prop.toString();

                        if(studentNumber is int && proposalApproved is string ){
                            assignedProposals[studentNumber.toString()] = {studentNumber, proposalApproved};
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

function extractFinal(string topic){
    kafka:ConsumerRecord[] records = checkpanic consumer->poll(1000);

    foreach var kafkaRecord in records {
        if(kafkaRecord.offset.partition.topic == topic){
            byte[] messageContent = kafkaRecord.value;
            string|error message = string:fromBytes(messageContent);

            if (message is string) {
                json|error jsonContent = message.fromJsonString();

                if(jsonContent is json){
                    json|error stN = jsonContent.studentNumber;
                    json|error appr = jsonContent.approved;


                    if(stN is json && appr is json){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error approved = appr.toString();

                        if(studentNumber is int && approved is string ){
                            assignedProposals[studentNumber.toString()] = {studentNumber, approved, finalSelection: "true"};
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




