import { createServer, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { createTestPluginServiceScheduler } from "openclaw/plugin-sdk/plugin-test-api";
import type { RuntimeEnv } from "openclaw/plugin-sdk/runtime";
import { afterEach, expect, it, vi } from "vitest";
import { useTlonMonitorFixture } from "./monitor.test-harness.js";

const {
  monitorTlonProvider,
  authenticateMock,
  sleepWithAbortMock,
  ingressMock,
  inboundRuntimeMock,
  settingsManagerMock,
  realUrbitFixture,
} = useTlonMonitorFixture();

const COOKIE = "urbauth-~zod=proof";
const SOURCE_NEST = "chat/~zod/citations";
const FOREIGN_NEST = "chat/~private/leadership";
const POST_ID = "12345";
const runningServers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    runningServers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
          server.closeAllConnections();
        }),
    ),
  );
});

function citationContent(nest: string) {
  return [
    { block: { cite: { chan: { nest, where: `/msg/~bus/${POST_ID}` } } } },
    { inline: ["~zod inspect this citation"] },
  ];
}

it("keeps admitted citation reads inside the source conversation at the authenticated transport", async () => {
  const controller = new AbortController();
  const subscriptionsByApp = new Map<string, number>();
  const citedPostRequests: Array<{ cookie: string | undefined; pathname: string }> = [];
  let stream: ServerResponse | undefined;
  const streamOpened = Promise.withResolvers<ServerResponse>();
  const runtime = { error: vi.fn(), exit: vi.fn(), log: vi.fn() } satisfies RuntimeEnv;

  const server = createServer((req, res) => {
    const pathname = new URL(req.url ?? "/", "http://127.0.0.1").pathname;
    if (req.method === "POST" && pathname === "/~/login") {
      res.writeHead(200, { "Set-Cookie": `${COOKIE}; Path=/; HttpOnly` }).end("ok");
      return;
    }
    if (req.method === "GET" && pathname.startsWith("/~/scry/")) {
      if (pathname.includes("/posts/post/")) {
        citedPostRequests.push({ cookie: req.headers.cookie, pathname });
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        pathname.includes("/posts/post/")
          ? JSON.stringify({ essay: { content: [{ inline: ["CITED-CONTENT"] }] } })
          : "{}",
      );
      return;
    }
    if (req.method === "PUT" && pathname.startsWith("/~/channel/")) {
      let body = "";
      req.setEncoding("utf8");
      req.on("data", (part: string) => {
        body += part;
      });
      req.on("end", () => {
        const actions = JSON.parse(body) as Array<{
          action?: string;
          app?: string;
          id?: number;
        }>;
        for (const action of actions) {
          if (action.action === "subscribe" && action.app && typeof action.id === "number") {
            subscriptionsByApp.set(action.app, action.id);
          }
        }
        res.writeHead(204).end();
      });
      return;
    }
    if (req.method === "GET" && pathname.startsWith("/~/channel/")) {
      res.writeHead(200, {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
      });
      res.write(": connected\n\n");
      streamOpened.resolve(res);
      return;
    }
    if (req.method === "DELETE" && pathname.startsWith("/~/channel/")) {
      res.writeHead(204).end();
      return;
    }
    res.writeHead(404).end();
  });
  runningServers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address() as AddressInfo;
  realUrbitFixture.url = `http://127.0.0.1:${address.port}`;
  realUrbitFixture.enabled = true;
  realUrbitFixture.config = {
    channels: {
      tlon: {
        code: "proof",
        defaultAuthorizedShips: ["~bus"],
        dmAllowlist: ["~bus"],
        groupChannels: [SOURCE_NEST],
        network: { dangerouslyAllowPrivateNetwork: true },
        ownerShip: "~nec",
        ship: "~zod",
        url: realUrbitFixture.url,
      },
    },
  };
  ingressMock.receive.mockResolvedValue({ kind: "ignored" });
  settingsManagerMock.load.mockResolvedValue({});
  const actualAuth = await vi.importActual<typeof import("../urbit/auth.js")>("../urbit/auth.js");
  const actualRuntimeEnv = await vi.importActual<typeof import("openclaw/plugin-sdk/runtime-env")>(
    "openclaw/plugin-sdk/runtime-env",
  );
  authenticateMock.mockImplementation(actualAuth.authenticate);
  sleepWithAbortMock.mockImplementation(actualRuntimeEnv.sleepWithAbort);

  const scheduler = createTestPluginServiceScheduler();
  const monitor = monitorTlonProvider({
    scheduler,
    abortSignal: controller.signal,
    runtime,
  });

  try {
    stream = await streamOpened.promise;
    await vi.waitFor(() => {
      expect(subscriptionsByApp.get("channels")).toBeTypeOf("number");
      expect(subscriptionsByApp.get("chat")).toBeTypeOf("number");
    });
    const channelsSubId = subscriptionsByApp.get("channels")!;
    const chatSubId = subscriptionsByApp.get("chat")!;
    const writeEvent = (id: number, subscriptionId: number, json: unknown) => {
      stream?.write(`id: ${id}\ndata: ${JSON.stringify({ id: subscriptionId, json })}\n\n`);
    };

    writeEvent(1, channelsSubId, {
      nest: SOURCE_NEST,
      response: {
        post: {
          id: "same-channel",
          "r-post": {
            set: {
              essay: { author: "~bus", content: citationContent(SOURCE_NEST), sent: 1 },
            },
          },
        },
      },
    });
    writeEvent(2, chatSubId, {
      whom: "~bus",
      id: "dm-citation",
      response: {
        add: { essay: { author: "~bus", content: citationContent(SOURCE_NEST), sent: 2 } },
      },
    });
    writeEvent(3, channelsSubId, {
      nest: SOURCE_NEST,
      response: {
        post: {
          id: "cross-channel",
          "r-post": {
            set: { essay: { author: "~bus", content: citationContent(FOREIGN_NEST), sent: 3 } },
          },
        },
      },
    });

    await vi.waitFor(() => expect(inboundRuntimeMock.dispatch).toHaveBeenCalledTimes(3), {
      timeout: 5_000,
    });
    expect(citedPostRequests).toEqual([
      {
        cookie: COOKIE,
        pathname: `/~/scry/channels/v4/${SOURCE_NEST}/posts/post/${POST_ID}.json`,
      },
    ]);
    const rawBodies = inboundRuntimeMock.buildContext.mock.calls.map(
      ([params]) => params.message.rawBody,
    );
    expect(rawBodies).toEqual([
      `> ~bus wrote: CITED-CONTENT\n\n> [quoted: ~bus in ${SOURCE_NEST}]\n\n~zod inspect this citation`,
      `> [quoted: ~bus in ${SOURCE_NEST}]\n\n~zod inspect this citation`,
      `> [quoted: ~bus in ${FOREIGN_NEST}]\n\n~zod inspect this citation`,
    ]);
    expect(rawBodies.slice(1)).not.toEqual(
      expect.arrayContaining([expect.stringContaining("CITED-CONTENT")]),
    );
    expect(runtime.error).not.toHaveBeenCalled();
  } finally {
    controller.abort();
    stream?.end();
    await monitor.catch(() => undefined);
    await scheduler.stop();
  }
});
