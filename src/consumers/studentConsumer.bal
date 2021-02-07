import ballerina/io;
import ballerina/http;
import ballerinax/kafka;
import ballerina/log;
import ballerina/docker;

@docker:Config{}
kafka:ConsumerConfiguration consumerConfiguration = {

    bootstrapServers: "localhost:9092",

    groupId: "student-group",
    offsetReset: "earliest",

    topics: ["hodSupervisorSelectionApproval", "supervisorProposalReview", "hdcThesisEndorsement", "hodFinalAdmission", "hdcThesisEndorsement"]

};

kafka:Consumer consumer = checkpanic new (consumerConfiguration);
http:Client clientEndpoint = check new ("http://localhost:9090");
type Info record {|
    int studentNumber;
    string approved;

|};
map<Info> info = {};

public function main() {
    while(true){
        io:println("Postgraduate Programme");
        io:println("*********Student*********");

        io:println("1. Application \n"
        + "2. Proposal \n"
        + "3. Thesis \n");

        string choice = io:readln("Enter choice 1 - 3: ");
        int c = checkpanic int:fromString(choice);

        if(c == 1){
            string studentNumber = io:readln("Enter your studentNumber: ");
            string name = io:readln("Enter your name: ");
            string course = io:readln("Enter your course: ");
            string application = io:readln("Enter your application: ");

            var  response = clientEndpoint->post("/graphql",{ query: " { apply(studentNumber:"+ studentNumber 
            + ",name: \""+ name +"\", course: \""+ course +"\", application: \""+ application + "\") }" });
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
            getMessages("hodSupervisorSelectionApproval");
            io:println(info);

            string studentNumber = io:readln("studentNumber: ");
            string proposal = io:readln("Enter your proposal: ");

            var  response = clientEndpoint->post("/graphql",{ query: " { propose(studentNumber:"+ studentNumber 
            + ",proposal: \""+ proposal +"\") }" });
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
                    json|error appr = jsonContent.approved;

                    if(stN is json && appr is json ){
                        int|error studentNumber = int:fromString(stN.toString());
                        string|error approved = appr.toString();


                        if(studentNumber is int && approved is string){
                            info[studentNumber.toString()] = {studentNumber, approved};
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


