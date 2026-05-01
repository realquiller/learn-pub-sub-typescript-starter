import amqp, { type ConfirmChannel } from "amqplib";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey, WarRecognitionsPrefix, GameLogSlug } from "../internal/routing/routing.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit, getMaliciousLog } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerMove, handlerPause, handlerConsumeWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { type GameLog } from "../internal/gamelogic/logs.js";

export function publishGameLog(ch: ConfirmChannel, username: string, message: string): Promise<void> {
  const logMessage: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };
  return publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, logMessage);
}

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);

  console.log("Connected to RabbitMQ");
  
  const username = await clientWelcome();
  console.log(`Client ${username} is ready.`);

  // create a new confirm channel using the .createConfirmChannel method on the connection.
  const channel = await conn.createConfirmChannel();
  console.log("Confirm channel created");

  const queueName = `pause.${username}`;

  const newGameState = new GameState(username);

  subscribeJSON(conn, ExchangePerilDirect, queueName, PauseKey, SimpleQueueType.Transient, handlerPause(newGameState));
  subscribeJSON(conn, ExchangePerilTopic, `army_moves.${username}`, `army_moves.*`, SimpleQueueType.Transient, handlerMove(newGameState, channel));
  subscribeJSON(conn, ExchangePerilTopic, "war", `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerConsumeWar(newGameState, channel));
  
   while (true) {
      const words = await getInput();
      if (words.length === 0 || !words[0]) {
        continue;
      }
  
      const command = words[0].toLowerCase();
      if (command === "spawn") {
        try {
          commandSpawn(newGameState, words);
        } catch (err) {
          console.log((err as Error).message);
        }
      }
      else if (command === "move") {
        try {
          const move = commandMove(newGameState, words);
          const message: ArmyMove = move;
          await publishJSON(channel, ExchangePerilTopic, `army_moves.${username}`, message);
          console.log("move was published successfully");
        } catch (err) {
          console.log((err as Error).message);
        }
      }
      else if (command === "status") {
        commandStatus(newGameState);
      }
      else if (command === "help") {
        printClientHelp();
      }
      else if (command === "spam") {
        if (words.length < 2) {
          console.log("Usage: spam <n> - where n is the number of messages to send");
          continue;
        }
        const n = parseInt(words[1]!, 10);
        if (isNaN(n) || n <= 0) {
          console.log("Please provide a valid positive integer for spam count");
          continue;
        }
        for (let i = 0; i < n; i++) {
          const message = getMaliciousLog();
          await publishGameLog(channel, username, message);
        }
        console.log(`Spam completed: ${n} messages sent`);
      }
      else if (command === "quit") {
        printQuit();
        process.exit(0);
      }
      else {
        console.log(`Unknown command: ${command}. Type 'help' for a list of commands.`);
        continue;
      }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
