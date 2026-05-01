import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey, ExchangePerilTopic, GameLogSlug } from "../internal/routing/routing.js";
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js";
import { subscribeMsgPack, SimpleQueueType } from "../internal/pubsub/consume.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlerLog } from "./handlers.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ");
  // create a new confirm channel using the .createConfirmChannel method on the connection.
  const channel = await conn.createConfirmChannel();
  console.log("Confirm channel created");

  // Subscribe to game_logs queue using msgpack with wildcard routing key
   subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    handlerLog(),
  );

  // WAIT FOR A SIGNAL E.G. SIGINT FROM CTRL+C TO EXIT THE PROGRAM GRACEFULLY
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT, shutting down...");
    await conn.close();
    console.log("RabbitMQ connection closed. Exiting.");
    process.exit(0);
  });
  
  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }
  printServerHelp();

  while (true) {
    const words = await getInput("Enter a command:\n");
    if (words.length === 0 || !words[0]) {
      continue;
    }

    
    const command = words[0].toLowerCase();
    if (command === "pause") {
      const message: PlayingState = { isPaused: true };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, message);
      console.log("I'm sending a pause message.");
    } else if (command === "resume") {
      const message: PlayingState = { isPaused: false };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, message);
      console.log("I'm sending a resume message.");
    } else if (command === "quit") {
      console.log("I'm exiting and breaking the loop.");
      break;
    } else {
      console.log(`Unknown command: ${command}. Type 'help' for a list of commands.`);
    }
  }

  


  
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
