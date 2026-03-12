const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "verification-service",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "verification-group" });
const producer = kafka.producer();

async function run(){

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "verification_request" });
  await consumer.subscribe({ topic: "rollback_verification" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {

      const data = JSON.parse(message.value);

      if(topic === "verification_request"){

        console.log("Verifying ID...");

        const success = true;

        await producer.send({
          topic: "verification_result",
          messages: [{
            value: JSON.stringify({
              ...data,
              status: success ? "success" : "failed"
            })
          }]
        });

      }

      if(topic === "rollback_verification"){

        console.log("Rollback verification triggered");

      }

    }
  });

}

run();
