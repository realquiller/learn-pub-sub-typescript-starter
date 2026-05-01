import amqp from "amqplib";
import type { Channel } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";
import { AckType } from "./subscribe.js";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
  Durable,
  Transient,
}


export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
  await channel.prefetch(10);
  await channel.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (!msg) {
      return;
    } else {
      try {
        const data = deserializer(msg.content);
        const result = await handler(data);
        if (result === AckType.Ack) {
          channel.ack(msg);
          console.log("Message processed and acknowledged");
        } else if (result === AckType.NackRequeue) {
          channel.nack(msg, false, true);
          console.log("Message processing failed, requeued for another attempt");
        } else if (result === AckType.NackDiscard) {
          channel.nack(msg, false, false);
          console.log("Message processing failed, discarded");
        }
      } catch (err) {
        console.error("Failed to process message:", err);
      }
    }
  });
}



export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const durable = queueType === SimpleQueueType.Durable;
  const autoDelete = queueType === SimpleQueueType.Transient;
  const exclusive = queueType === SimpleQueueType.Transient;

  const assertQueueResult = await channel.assertQueue(queueName, {
    durable, arguments: {
      "x-dead-letter-exchange": ExchangePerilDeadLetter
    },
    autoDelete,
    exclusive
  });
  await channel.bindQueue(assertQueueResult.queue, exchange, key);

  return [channel, assertQueueResult];
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, routingKey, simpleQueueType, handler, (data) => decode(data) as T);
}