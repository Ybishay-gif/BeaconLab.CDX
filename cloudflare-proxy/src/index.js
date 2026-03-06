export default {
  async fetch(request, env) {
    const primary = (env.ORIGIN_BASE || "").trim();
    const fallback = (env.ORIGIN_BASE_FALLBACK || "").trim();
    const origins = [primary, fallback].filter(Boolean);
    if (!origins.length) {
      return new Response("Missing ORIGIN_BASE (and optional ORIGIN_BASE_FALLBACK) worker variable.", { status: 500 });
    }

    const incoming = new URL(request.url);
    const bodyBuffer = request.method !== "GET" && request.method !== "HEAD" ? await request.arrayBuffer() : undefined;

    let response = null;
    let usedOrigin = "";
    const errors = [];

    for (const origin of origins) {
      const target = new URL(incoming.pathname + incoming.search, origin);
      const headers = new Headers(request.headers);
      headers.set("host", new URL(origin).host);

      const init = {
        method: request.method,
        headers,
        redirect: "follow"
      };

      if (bodyBuffer !== undefined) {
        init.body = bodyBuffer;
      }

      try {
        const attempt = await fetch(target.toString(), init);
        if (attempt.status < 500) {
          response = attempt;
          usedOrigin = origin;
          break;
        }
        errors.push(`${origin} returned ${attempt.status}`);
      } catch (_err) {
        errors.push(`${origin} unreachable`);
      }
    }

    if (!response) {
      return new Response(`All origins failed: ${errors.join("; ")}`, { status: 502 });
    }

    const outHeaders = new Headers(response.headers);
    outHeaders.set("x-proxy-target", usedOrigin);
    outHeaders.set("x-proxy-failover", usedOrigin === primary ? "false" : "true");
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: outHeaders
    });
  }
};
