import { Router } from "express";
import { Bot, webhookCallback } from "grammy";
import { config } from "../../config.js";
import { handleTelegramMessage, splitMessage } from "../../services/telegramBotService.js";

export const telegramRoutes = Router();

/* ------------------------------------------------------------------ */
/*  grammy Bot instance                                                */
/* ------------------------------------------------------------------ */

let bot: Bot | null = null;

function getBot(): Bot | null {
  if (bot) return bot;
  if (!config.telegramBotToken) return null;

  bot = new Bot(config.telegramBotToken);

  bot.command("start", async (ctx) => {
    const reply = await handleTelegramMessage(ctx.chat.id, "/start");
    await ctx.reply(reply);
  });

  bot.command("help", async (ctx) => {
    const reply = await handleTelegramMessage(ctx.chat.id, "/help");
    await ctx.reply(reply);
  });

  bot.command("clear", async (ctx) => {
    const reply = await handleTelegramMessage(ctx.chat.id, "/clear");
    await ctx.reply(reply);
  });

  bot.on("message:text", async (ctx) => {
    const text = ctx.message.text;
    const chatId = ctx.chat.id;

    try {
      const reply = await handleTelegramMessage(chatId, text);
      const chunks = splitMessage(reply);
      for (const chunk of chunks) {
        await ctx.reply(chunk, { parse_mode: "Markdown" });
      }
    } catch (err) {
      console.error("[telegram-bot] Error handling message:", err);
      await ctx.reply("Sorry, something went wrong. Please try again.");
    }
  });

  bot.catch((err) => {
    console.error("[telegram-bot] Bot error:", err.message);
  });

  return bot;
}

/* ------------------------------------------------------------------ */
/*  Webhook endpoint                                                   */
/* ------------------------------------------------------------------ */

telegramRoutes.post("/telegram/webhook/:secret", (req, res) => {
  // Validate webhook secret
  if (req.params.secret !== config.telegramWebhookSecret) {
    res.status(404).json({ error: "Not found" });
    return;
  }

  const botInstance = getBot();
  if (!botInstance) {
    res.status(503).json({ error: "Telegram bot not configured" });
    return;
  }

  // Delegate to grammy's webhook handler
  webhookCallback(botInstance, "express")(req, res);
});

/* ------------------------------------------------------------------ */
/*  Export for webhook registration                                    */
/* ------------------------------------------------------------------ */

export async function registerTelegramWebhook(baseUrl: string): Promise<void> {
  const botInstance = getBot();
  if (!botInstance || !config.telegramWebhookSecret) {
    console.log("[telegram-bot] Skipping webhook registration (not configured)");
    return;
  }

  const webhookUrl = `${baseUrl}/api/telegram/webhook/${config.telegramWebhookSecret}`;
  try {
    await botInstance.api.setWebhook(webhookUrl);
    console.log(`[telegram-bot] Webhook registered: ${webhookUrl.replace(config.telegramWebhookSecret, "***")}`);
  } catch (err) {
    console.error("[telegram-bot] Failed to register webhook:", err);
  }
}
