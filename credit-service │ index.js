const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "credit-service",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "credit-group" });
const producer = kafka.producer();

async function run(){

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "credit_request" });

  await consumer.run({
    eachMessage: async ({ message }) => {

      const data = JSON.parse(message.value);

      console.log("Checking credit score...");

      const approved = false;

      await producer.send({
        topic: "credit_result",
        messages: [{
          value: JSON.stringify({
            ...data,
            status: approved ? "approved" : "failed"
          })
        }]
      });

    }
  });

}

run();
