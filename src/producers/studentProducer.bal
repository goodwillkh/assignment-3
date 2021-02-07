import ballerinax/kafka;
import ballerina/graphql;
import ballerina/docker;

@docker:Expose {}
listener graphql:Listener studentListener = new(9090);

kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "studentProducer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = checkpanic new (producerConfiguration);

@docker:Config {
    name: "student",
    tag: "v1.0"
}
service graphql:Service /graphql on studentListener {

    resource function get apply(int studentNumber, string name, string course, string application) returns string {
        //apply
        string form = ({ studentNumber, name, course, application }).toString();

        checkpanic kafkaProducer->sendProducerRecord({
                                    topic: "studentApplication",
                                    value: form.toBytes() });

        checkpanic kafkaProducer->flushRecords();
        return "Appication sent!!";
    }
    
    //propose
    resource function get propose(int studentNumber, string proposal) returns string {
        
        string prop = ({ studentNumber, proposal }).toString();
        checkpanic kafkaProducer->sendProducerRecord({
                                    topic: "studentProposal",
                                    value: prop.toBytes() });

        checkpanic kafkaProducer->flushRecords();
        return "Proposal Submitted";
    }

    // resource function get thesis(string name, int voterID) returns string {
    //     //thesis
    //     checkpanic kafkaProducer->sendProducerRecord({
    //                                 topic: "studentThesis",
    //                                 value: message.toBytes() });

    //     checkpanic kafkaProducer->flushRecords();
    //     return "Registered candidate, " + name;
    // }

}


