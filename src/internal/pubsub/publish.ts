

// In publishJSON:
// Serialize the value to JSON bytes (e.g. JSON.stringify + Buffer.from).
// Use the channel's .publish method to publish the message to the exchange with the routing key.
// In the content parameter, send the new buffer created from the JSON bytes.
// For the options object, set the contentType to "application/json"

import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T
): Promise<void> {
  const jsonString = JSON.stringify(value);
  const jsonBuffer = Buffer.from(jsonString, "utf-8");

  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, jsonBuffer, { contentType: "application/json" }, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const msgPackBuffer = Buffer.from(encode(value));
  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, msgPackBuffer, { contentType: "application/x-msgpack" }, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}
