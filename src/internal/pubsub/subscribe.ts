import { SimpleQueueType, subscribe } from "./consume.js";
import amqp from "amqplib";

export enum AckType {
  Ack = 'Ack',
  NackRequeue = 'NackRequeue',
  NackDiscard = 'NackDiscard',
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, routingKey, simpleQueueType, handler, (data) => JSON.parse(data.toString()) as T);
}