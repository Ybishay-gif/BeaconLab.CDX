import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import request from "supertest";
import { analyticsRoutes } from "./analyticsRoutes.js";
import { targetsRoutes } from "./targetsRoutes.js";
function buildApp() {
    const app = express();
    app.use(express.json());
    app.use(analyticsRoutes);
    app.use(targetsRoutes);
    return app;
}
test("GET /analytics/state-segment-performance requires qbc", async () => {
    const app = buildApp();
    const res = await request(app).get("/analytics/state-segment-performance");
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
test("GET /analytics/price-exploration requires qbc", async () => {
    const app = buildApp();
    const res = await request(app).get("/analytics/price-exploration");
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
test("GET /analytics/strategy-analysis requires qbc", async () => {
    const app = buildApp();
    const res = await request(app).get("/analytics/strategy-analysis?planId=plan-1");
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
test("GET /analytics/state-analysis requires qbc", async () => {
    const app = buildApp();
    const res = await request(app).get("/analytics/state-analysis?planId=plan-1");
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
test("GET /targets requires qbc", async () => {
    const app = buildApp();
    const res = await request(app).get("/targets");
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
test("POST /targets/metrics requires qbc", async () => {
    const app = buildApp();
    const res = await request(app)
        .post("/targets/metrics")
        .send({ rows: [{ state: "CA", segment: "MCH", source: "Source A" }] });
    assert.equal(res.status, 400);
    assert.equal(res.body?.error, "qbc is required");
});
