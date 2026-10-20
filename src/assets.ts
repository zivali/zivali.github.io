import fs from "fs";
import fsExtra from "fs-extra";
import http from "http";
import https from "https";
import path from "path";

const ASSET_ROOT = path.join("static", "images", "notion");
const PUBLIC_ROOT = "/images/notion";
const MAX_REDIRECTS = 5;
const MAX_BYTES = 20 * 1024 * 1024;
const REQUEST_TIMEOUT_MS = 30_000;

type DownloadedResponse = {
  body: Buffer;
  contentType: string | undefined;
};

function safePart(value: string): string {
  const part = value.replace(/[^a-zA-Z0-9_-]/g, "");
  if (!part) throw new Error(`Invalid Notion asset path part: ${value}`);
  return part;
}

function extensionFromContentType(contentType: string | undefined): string | undefined {
  const type = contentType?.split(";", 1)[0].toLowerCase();
  const extensions: Record<string, string> = {
    "image/avif": "avif",
    "image/gif": "gif",
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/svg+xml": "svg",
    "image/webp": "webp",
  };
  return type ? extensions[type] : undefined;
}

function extensionFromUrl(url: string): string | undefined {
  const extension = path.extname(new URL(url).pathname).slice(1).toLowerCase();
  return /^[a-z0-9]{1,5}$/.test(extension) ? extension : undefined;
}

function request(url: string, redirects = 0): Promise<DownloadedResponse> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      reject(new Error(`Unsupported asset URL protocol: ${parsed.protocol}`));
      return;
    }

    const transport = parsed.protocol === "https:" ? https : http;
    const req = transport.get(parsed, (response) => {
      const status = response.statusCode ?? 0;
      if (status >= 300 && status < 400 && response.headers.location) {
        response.resume();
        if (redirects >= MAX_REDIRECTS) {
          reject(new Error(`Too many redirects while downloading ${url}`));
          return;
        }
        resolve(request(new URL(response.headers.location, parsed).toString(), redirects + 1));
        return;
      }
      if (status < 200 || status >= 300) {
        response.resume();
        reject(new Error(`Failed to download ${url}: HTTP ${status}`));
        return;
      }

      const chunks: Buffer[] = [];
      let size = 0;
      response.on("data", (chunk: Buffer) => {
        size += chunk.length;
        if (size > MAX_BYTES) {
          req.destroy(new Error(`Asset is larger than ${MAX_BYTES} bytes: ${url}`));
          return;
        }
        chunks.push(chunk);
      });
      response.on("end", () => resolve({
        body: Buffer.concat(chunks),
        contentType: response.headers["content-type"],
      }));
      response.on("error", reject);
    });
    req.setTimeout(REQUEST_TIMEOUT_MS, () => req.destroy(new Error(`Timed out downloading ${url}`)));
    req.on("error", reject);
  });
}

export async function downloadNotionAsset(
  url: string,
  pageId: string,
  assetKey: string,
  staticRoot = ASSET_ROOT,
): Promise<string> {
  const response = await request(url);
  const extension = extensionFromContentType(response.contentType) ?? extensionFromUrl(url) ?? "bin";
  const safePageId = safePart(pageId.replace(/-/g, ""));
  const safeKey = safePart(assetKey);
  const directory = path.join(staticRoot, safePageId);
  const filename = `${safeKey}.${extension}`;
  const destination = path.join(directory, filename);
  const temporary = `${destination}.${process.pid}.tmp`;

  await fsExtra.ensureDir(directory);
  try {
    await fs.promises.writeFile(temporary, response.body);
    await fs.promises.rename(temporary, destination);
  } finally {
    await fs.promises.rm(temporary, { force: true });
  }
  return `${PUBLIC_ROOT}/${safePageId}/${filename}`;
}
