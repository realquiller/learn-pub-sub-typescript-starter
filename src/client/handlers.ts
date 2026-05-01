import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { GameState , type PlayingState} from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { WarRecognitionsPrefix, ExchangePerilTopic } from "../internal/routing/routing.js";
import type { ConfirmChannel } from "amqplib";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState, channel: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove): Promise<AckType> => {
    const outcome = handleMove(gs, move);
    process.stdout.write("> ");
    if (outcome === MoveOutcome.MakeWar) {
      const routingKey = `${WarRecognitionsPrefix}.${gs.getUsername()}`;
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap()
      };
      
      try {
        await publishJSON(channel, ExchangePerilTopic, routingKey, rw);
      } catch (err) {
          return AckType.NackRequeue;
      }

      return AckType.Ack;
      

      

    }
    
    if (outcome === MoveOutcome.Safe) {
      return AckType.Ack;
    } else {
      return AckType.NackDiscard;
    }
  };
}

export function handlerConsumeWar (gs: GameState, channel: ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar): Promise<AckType> => {
    try {
      const outcome = handleWar(gs, rw);
      const result = outcome.result;
      if (result === WarOutcome.NotInvolved) {
        return AckType.NackDiscard;
      } else if (result === WarOutcome.NoUnits) {
        return AckType.NackDiscard;
      } else if (result === WarOutcome.OpponentWon) {
        try {
          await publishGameLog(channel, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`);
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        }
        
      } else if (result === WarOutcome.YouWon) {
        try {
          await publishGameLog(channel, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`);
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        }
        
      } else if (result === WarOutcome.Draw) {
        try {
          await publishGameLog(channel, gs.getUsername(), `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`); 
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        }
      }
      else {
        return AckType.NackDiscard;
      }
    } finally {
        process.stdout.write("> ");
    }
    
  };
}