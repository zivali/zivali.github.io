import http from "http";
import fs from "fs-extra";
import os from "os";
import path from "path";
import { afterEach, describe, expect, it } from "vitest";
import { downloadNotionAsset } from "./assets";

const servers: http.Server[] = [];
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
  await Promise.all(temporaryDirectories.splice(0).map((directory) => fs.remove(directory)));
});

async function serve(statusCode: number, body: string, contentType: string): Promise<string> {
  const server = http.createServer((_request, response) => {
    response.writeHead(statusCode, { "content-type": contentType });
    response.end(body);
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Test server did not start");
  return `http://127.0.0.1:${address.port}/image`;
}

async function tempDirectory(): Promise<string> {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "notion-assets-"));
  temporaryDirectories.push(directory);
  return directory;
}

describe("downloadNotionAsset", () => {
  it("downloads bytes to a stable Hugo asset path", async () => {
    const root = await tempDirectory();
    const url = await serve(200, "image-bytes", "image/png");

    const publicPath = await downloadNotionAsset(url, "page-id", "image-block-id", root);

    expect(publicPath).toBe("/images/notion/pageid/image-block-id.png");
    await expect(fs.readFile(path.join(root, "pageid", "image-block-id.png"), "utf8"))
      .resolves.toBe("image-bytes");
  });

  it("does not replace an existing asset after a failed download", async () => {
    const root = await tempDirectory();
    const url = await serve(500, "error", "text/plain");
    const destination = path.join(root, "pageid", "image-block-id.png");
    await fs.ensureDir(path.dirname(destination));
    await fs.writeFile(destination, "old-image");

    await expect(downloadNotionAsset(url, "page-id", "image-block-id", root)).rejects.toThrow("HTTP 500");
    await expect(fs.readFile(destination, "utf8")).resolves.toBe("old-image");
  });
});
