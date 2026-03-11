// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import assert from "node:assert";
import childProcess from "node:child_process";
import events from "node:events";
import { readFileSync, readdirSync } from "node:fs";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import ts from "typescript";
import { SourcesMap, createMemoryProgram } from "../src/program";
import { getFilePath } from "../src/utils";

// Build a PROXY v2 header with a worker ID TLV (type 0xE0).
// Format: signature(12) + ver/cmd(1) + fam(1) + len(2) + TLV(3 + workerId.length)
function buildProxyV2Header(workerId: string): Buffer {
  const workerIdBuf = Buffer.from(workerId);
  const payloadLen = 3 + workerIdBuf.length;
  const header = Buffer.alloc(16 + payloadLen);
  // PROXY v2 signature
  Buffer.from([0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a]).copy(header, 0);
  header[12] = 0x20;  // v2, LOCAL command
  header[13] = 0x00;  // AF_UNSPEC
  header.writeUInt16BE(payloadLen, 14);
  header[16] = 0xe0;  // PP2_TYPE_WORKER_ID
  header.writeUInt16BE(workerIdBuf.length, 17);
  workerIdBuf.copy(header, 19);
  return header;
}

// fetch() replacement that sends a PROXY v2 header before the HTTP request.
// Uses a raw TCP connection with manual HTTP/1.1 formatting.
async function proxyV2Fetch(url: string | URL, workerId: string): Promise<Response> {
  const parsed = new URL(url);
  const socket = net.connect(parseInt(parsed.port), parsed.hostname!);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });

  // Send PROXY v2 header followed by the HTTP request.
  socket.write(buildProxyV2Header(workerId));
  socket.write(`GET ${parsed.pathname}${parsed.search} HTTP/1.1\r\nHost: ${parsed.host}\r\nConnection: close\r\n\r\n`);

  // Read the full response as raw bytes.
  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    socket.on("data", (chunk: Buffer) => chunks.push(chunk));
    socket.on("end", resolve);
    socket.on("error", reject);
  });
  const raw = Buffer.concat(chunks);

  // Find end of HTTP headers (search in buffer for \r\n\r\n).
  const separator = Buffer.from("\r\n\r\n");
  const headerEnd = raw.indexOf(separator);
  if (headerEnd < 0) {
    throw new Error("Invalid HTTP response: no header terminator found");
  }

  const headerSection = raw.subarray(0, headerEnd).toString();
  const body = raw.subarray(headerEnd + 4);
  const [statusLine, ...headerLines] = headerSection.split("\r\n");
  const statusMatch = statusLine.match(/HTTP\/\d+\.\d+\s+(\d+)/);
  const statusCode = statusMatch ? parseInt(statusMatch[1]) : 500;

  const headers = new Headers();
  for (const line of headerLines) {
    const colonIdx = line.indexOf(":");
    if (colonIdx > 0) {
      headers.append(line.substring(0, colonIdx).trim(), line.substring(colonIdx + 1).trim());
    }
  }

  return new Response(body, { status: statusCode, headers });
}

const OUTPUT_PATH = getFilePath("types/definitions");
const ENTRYPOINTS = [
  { compatDate: "2021-01-01", name: "oldest" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#formdata-parsing-supports-file
  { compatDate: "2021-11-03" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#settersgetters-on-api-object-prototypes
  { compatDate: "2022-01-31" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#global-navigator
  { compatDate: "2022-03-21" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#r2-bucket-list-respects-the-include-option
  { compatDate: "2022-08-04" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#new-url-parser-implementation
  { compatDate: "2022-10-31" },
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#streams-constructors
  // https://developers.cloudflare.com/workers/platform/compatibility-dates/#compliant-transformstream-constructor
  { compatDate: "2022-11-30" },
  // https://github.com/cloudflare/workerd/blob/fcb6f33d10c71975cb2ce68dbf1924a1eeadbd8a/src/workerd/io/compatibility-date.capnp#L275-L280 (http_headers_getsetcookie)
  { compatDate: "2023-03-01" },
  // https://github.com/cloudflare/workerd/blob/fcb6f33d10c71975cb2ce68dbf1924a1eeadbd8a/src/workerd/io/compatibility-date.capnp#L307-L312 (urlsearchparams_delete_has_value_arg)
  { compatDate: "2023-07-01" },
  // Latest compatibility date with experimental features
  { compatDate: "2999-12-31", name: "latest" },
  // Latest compatibility date with experimental features
  { compatDate: "experimental" },
];

/**
 * Copy all TS lib files into the memory filesystem. We only use lib.esnext
 * but since TS lib files all reference each other to various extents it's
 * easier to add them all and let TS figure out which ones it actually needs to load.
 * This function uses the current local installation of TS as a source for lib files
 */
let cachedLibFiles: SourcesMap | null = null;
function loadLibFiles(): SourcesMap {
  if (cachedLibFiles != null) {
    return cachedLibFiles;
  }

  const libLocation = path.dirname(require.resolve("typescript"));
  const libFiles = readdirSync(libLocation).filter(
    (file) => file.startsWith("lib.") && file.endsWith(".d.ts")
  );
  const lib: SourcesMap = new Map();
  for (const file of libFiles) {
    lib.set(
      `/node_modules/typescript/lib/${file}`,
      readFileSync(path.join(libLocation, file), "utf-8")
    );
  }

  cachedLibFiles = lib;
  return lib;
}

function checkDiagnostics(sources: SourcesMap): void {
  const program = createMemoryProgram(
    sources,
    undefined,
    {
      noEmit: true,
      lib: ["lib.esnext.d.ts"],
      types: [],
      noUnusedParameters: true,
    },
    loadLibFiles()
  );

  const emitResult = program.emit();

  const allDiagnostics = ts
    .getPreEmitDiagnostics(program)
    .concat(emitResult.diagnostics);

  allDiagnostics.forEach((diagnostic) => {
    if (diagnostic.file) {
      const { line, character } = ts.getLineAndCharacterOfPosition(
        diagnostic.file,
        diagnostic.start
      );
      const message = ts.flattenDiagnosticMessageText(
        diagnostic.messageText,
        "\n"
      );
      console.log(
        `${diagnostic.file.fileName}:${line + 1}:${character + 1} : ${message}`
      );
    } else {
      console.log(
        ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")
      );
    }
  });

  assert(allDiagnostics.length === 0, "TypeScript failed to compile!");
}

function spawnWorkerd(
  configPath: string
): Promise<{ url: URL; kill: () => Promise<void> }> {
  return new Promise((resolve) => {
    const workerdProcess = childProcess.spawn(
      getFilePath("src/workerd/server/workerd"),
      ["serve", "--verbose", "--experimental", "--control-fd=3", configPath],
      { stdio: ["inherit", "inherit", "inherit", "pipe"] }
    );
    const exitPromise = events.once(workerdProcess, "exit");
    workerdProcess.stdio[3]?.on("data", (chunk: Buffer): void => {
      const message = JSON.parse(chunk.toString().trim()) as {
        event: string
        port: number
      };
      assert.strictEqual(message.event, "listen");
      resolve({
        url: new URL(`http://127.0.0.1:${message.port}`),
        async kill() {
          workerdProcess.kill("SIGTERM");
          await exitPromise;
        },
      });
    });
  });
}

async function buildEntrypoint(
  entrypoint: (typeof ENTRYPOINTS)[number],
  workerUrl: URL
): Promise<{ name: string; files: Array<{ fileName: string; content: string }> }> {
  const url = new URL(`/${entrypoint.compatDate}.bundle`, workerUrl);
  const response = await proxyV2Fetch(url, "main");
  if (!response.ok) throw new Error(await response.text());
  const bundle = await response.formData();

  const name = entrypoint.name ?? entrypoint.compatDate;
  const entrypointPath = path.join(OUTPUT_PATH, name);
  await fs.mkdir(entrypointPath, { recursive: true });

  const files: Array<{ fileName: string; content: string }> = [];

  const filePromises: Promise<void>[] = [];

  for (const [fileName, definitions] of bundle) {
    assert(typeof definitions === "string");
    const prettierIgnoreRegexp = /^\s*\/\/\s*prettier-ignore\s*\n/gm;
    const typings = definitions.replaceAll(prettierIgnoreRegexp, "");

    files.push({ fileName, content: typings });
    filePromises.push(fs.writeFile(path.join(entrypointPath, fileName), typings));
  }

  // Write all files in parallel (without prettier formatting)
  await Promise.all(filePromises);

  return { name, files };
}

async function buildAllEntrypoints(workerUrl: URL): Promise<void> {
  const allEntrypoints = await Promise.all(
    ENTRYPOINTS.map(entrypoint => buildEntrypoint(entrypoint, workerUrl))
  );

  // Format all TypeScript files with a single Prettier CLI call using exact same defaults as API
  const prettierPath = require.resolve("prettier/bin/prettier.cjs");
  childProcess.execSync(`${prettierPath} "${OUTPUT_PATH}/**/*.ts" --write --parser=typescript`);

  for (const { files } of allEntrypoints) {
    const entrypointFiles = new SourcesMap();
    for (const { fileName, content } of files) {
      entrypointFiles.set(`/$virtual/${fileName}`, content);
    }

    if (entrypointFiles.size > 0) {
      checkDiagnostics(entrypointFiles);
    }
  }
}
export async function main(): Promise<void> {
  const worker = await spawnWorkerd(getFilePath("types/scripts/config.capnp"));
  try {
    await buildAllEntrypoints(worker.url);
  } finally {
    await worker.kill();
  }
}

// Outputting to a CommonJS module so can't use top-level await
if (require.main === module) void main();
