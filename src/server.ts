import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ZodError } from "zod";
import { config } from "./config.js";
import { healthRouter } from "./routes/health.js";
import { plansRouter } from "./routes/plans.js";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.json());
app.use(express.static(path.resolve(__dirname, "../public")));

app.use(healthRouter);
app.use("/api", plansRouter);

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  if (err instanceof ZodError) {
    res.status(400).json({ error: "Invalid request", details: err.issues });
    return;
  }

  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  if (typeof status === "number" && status >= 400 && status < 600) {
    const message =
      typeof err === "object" && err !== null && "message" in err ? String((err as { message: unknown }).message) : "";
    res.status(status).json({ error: message || "Request failed" });
    return;
  }

  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

app.get("*", (_req, res) => {
  res.sendFile(path.resolve(__dirname, "../public/index.html"));
});

app.listen(config.port, () => {
  console.log(`planning-app-api listening on port ${config.port}`);
});
