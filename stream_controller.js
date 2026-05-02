const path = require("node:path");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const { spawn } = require("node:child_process");
const crypto = require("node:crypto");
const Fastify = require("fastify");
const cors = require("@fastify/cors");
require('dotenv').config();

const app = Fastify({ logger: true });

const ROOT_DIR = __dirname;
const RUNTIME_DIR = path.join(ROOT_DIR, "runtime");
const SCRIPTS_DIR = path.join(RUNTIME_DIR, "scripts");
const LOGS_DIR = path.join(RUNTIME_DIR, "logs");
const PLAYLISTS_DIR = path.join(RUNTIME_DIR, "playlists");
const NORMALIZED_DIR = path.join(RUNTIME_DIR, "normalized");
const DOWNLOADS_DIR = path.join(ROOT_DIR, "downloads");

const LIQUIDSOAP_BIN = process.env.LIQUIDSOAP_BIN || "liquidsoap";
const HOST = process.env.STREAM_API_HOST || "0.0.0.0";
const PORT = Number(process.env.STREAM_API_PORT || 8090);

// API_KEY mandatory — না থাকলে boot-এই crash করবে
const API_KEY = process.env.STREAM_API_KEY;
if (!API_KEY || API_KEY.trim() === "") {
  console.error("[fatal] STREAM_API_KEY environment variable is required but not set.");
  console.error("[fatal] Set it before starting: STREAM_API_KEY=your-secret-key node stream_controller.js");
  process.exit(1);
}

// Stream শুরু করার জন্য minimum bytes (5MB)
const MIN_BYTES_TO_START = 5 * 1024 * 1024;

/** @type {Map<string, {
 *   id: string,
 *   process: import("node:child_process").ChildProcessWithoutNullStreams,
 *   liqPath: string,
 *   logPath: string,
 *   playlistPath: string,
 *   downloadedFiles: string[],
 *   createdAt: number,
 *   config: object
 * }>} */
const jobs = new Map();

// ─── Helpers ──────────────────────────────────────────────────────────────────

function toLiquidsoapPath(inputPath) {
  return String(inputPath).replaceAll("\\", "/");
}

function toOsPath(liqPath) {
  if (process.platform === "win32") {
    return String(liqPath).replaceAll("/", "\\");
  }
  return liqPath;
}

function ensureDirs() {
  for (const dir of [
    RUNTIME_DIR, SCRIPTS_DIR, LOGS_DIR,
    PLAYLISTS_DIR, NORMALIZED_DIR, DOWNLOADS_DIR,
  ]) {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  }
}

function makeScript(payload, playlistPath) {
  let url = payload.url;
  const streamKey = payload.stream_key;
  if (!url && streamKey) url = `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
  if (!url) throw new Error("Provide either `url` or `stream_key`.");

  const copyMode = payload.copy_mode === true;
  const avLine = copyMode
    ? "%video.copy, %audio.copy"
    : '%video(codec="libx264"), %audio(codec="aac")';
  const playlistPathLs = toLiquidsoapPath(playlistPath);

  return `settings.log.stdout := true

src = playlist(reload_mode="watch", "${playlistPathLs}")

output.url(
  %ffmpeg(format="flv", ${avLine}),
  url="${url}",
  fallible=true,
  src
)
`;
}

function jobSummary(job) {
  return {
    id: job.id,
    pid: job.process.pid,
    running: job.process.exitCode === null,
    created_at: job.createdAt,
    liq_path: job.liqPath,
    log_path: job.logPath,
    playlist_path: job.playlistPath,
    files_count: Array.isArray(job.config?.files) ? job.config.files.length : 0,
  };
}

async function readLastLines(filePath, count = 120) {
  try {
    const text = await fsp.readFile(filePath, "utf8");
    return text.split(/\r?\n/).slice(-count).join("\n");
  } catch (err) {
    if (err?.code === "ENOENT") return "";
    throw err;
  }
}

async function writePlaylistFile(playlistPath, resolvedFiles) {
  if (!Array.isArray(resolvedFiles) || resolvedFiles.length === 0) {
    throw new Error("`files` array is required and cannot be empty.");
  }
  await fsp.writeFile(playlistPath, `${resolvedFiles.join("\n")}\n`, "utf8");
}

function isRemoteUrl(filePath) {
  return typeof filePath === "string" && /^https?:\/\//i.test(filePath);
}

// ─── Download with partial-ready callback ─────────────────────────────────────

function downloadFileProgressive(url, destPath, id, onPartialReady) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith("https") ? require("https") : require("http");
    let partialFired = false;
    let bytesReceived = 0;

    fsp.mkdir(path.dirname(destPath), { recursive: true }).then(() => {
      const file = fs.createWriteStream(destPath);

      const req = proto.get(url, (res) => {
        // Redirect
        if ([301, 302, 307, 308].includes(res.statusCode)) {
          file.close();
          fsp.unlink(destPath).catch(() => {});
          return downloadFileProgressive(res.headers.location, destPath, id, onPartialReady)
            .then(resolve)
            .catch(reject);
        }

        if (res.statusCode !== 200) {
          file.close();
          fsp.unlink(destPath).catch(() => {});
          return reject(new Error(`Download failed: HTTP ${res.statusCode} for ${url}`));
        }

        res.on("data", (chunk) => {
          bytesReceived += chunk.length;

          if (!partialFired && bytesReceived >= MIN_BYTES_TO_START) {
            partialFired = true;
            console.log(`[${id}] Partial ready at ${(bytesReceived / 1024 / 1024).toFixed(1)}MB — starting stream`);
            onPartialReady(destPath);
          }
        });

        res.pipe(file);

        file.on("finish", () => {
          file.close(() => {
            if (!partialFired) {
              partialFired = true;
              console.log(`[${id}] File complete (${(bytesReceived / 1024 / 1024).toFixed(1)}MB) — starting stream`);
              onPartialReady(destPath);
            }
            resolve();
          });
        });

        file.on("error", (err) => {
          fsp.unlink(destPath).catch(() => {});
          reject(err);
        });
      });

      req.on("error", (err) => {
        file.close();
        fsp.unlink(destPath).catch(() => {});
        reject(err);
      });

      req.setTimeout(120000, () => {
        req.destroy();
        file.close();
        fsp.unlink(destPath).catch(() => {});
        reject(new Error(`Download timeout for ${url}`));
      });
    }).catch(reject);
  });
}

// ─── Progressive resolve ──────────────────────────────────────────────────────

async function resolveFilesProgressive(id, files, playlistPath, onStreamReady) {
  const resolved = new Array(files.length).fill(null);
  const downloadedLocalPaths = [];
  let streamStarted = false;

  async function maybeStartStream() {
    if (streamStarted || resolved[0] === null) return;
    streamStarted = true;
    await writePlaylistFile(playlistPath, resolved.filter(Boolean));
    await onStreamReady(resolved.filter(Boolean));
  }

  async function updatePlaylistIfRunning() {
    const readyFiles = resolved.filter(Boolean);
    if (readyFiles.length > 0) {
      try {
        await writePlaylistFile(playlistPath, readyFiles);
      } catch (err) {
        console.error(`[${id}] Playlist update error: ${err.message}`);
      }
    }
  }

  // প্রথম file — partial ready হলেই stream start
  const firstFilePromise = (async () => {
    const file = files[0];
    if (!isRemoteUrl(file)) {
      resolved[0] = toLiquidsoapPath(file);
      await maybeStartStream();
      return;
    }

    const ext = path.extname(new URL(file).pathname) || ".mp4";
    const hash = crypto.createHash("md5").update(file).digest("hex").slice(0, 8);
    const tmpPath = path.join(DOWNLOADS_DIR, `${id}_${hash}${ext}`);

    try {
      await fsp.access(tmpPath);
      console.log(`[${id}] Already cached [0]: ${tmpPath}`);
      downloadedLocalPaths.push(tmpPath);
      resolved[0] = toLiquidsoapPath(tmpPath);
      await maybeStartStream();
      return;
    } catch { /* not cached, download */ }

    console.log(`[${id}] Downloading [0]: ${file}`);

    await downloadFileProgressive(file, tmpPath, id, async () => {
      downloadedLocalPaths.push(tmpPath);
      resolved[0] = toLiquidsoapPath(tmpPath);
      await maybeStartStream();
    });

    console.log(`[${id}] Complete [0]: ${tmpPath}`);
    await updatePlaylistIfRunning();
  })();

  // বাকি files — background download
  const restPromises = files.slice(1).map(async (file, idx) => {
    const i = idx + 1;

    if (!isRemoteUrl(file)) {
      resolved[i] = toLiquidsoapPath(file);
      await updatePlaylistIfRunning();
      return;
    }

    const ext = path.extname(new URL(file).pathname) || ".mp4";
    const hash = crypto.createHash("md5").update(file).digest("hex").slice(0, 8);
    const tmpPath = path.join(DOWNLOADS_DIR, `${id}_${hash}${ext}`);

    try {
      await fsp.access(tmpPath);
      console.log(`[${id}] Already cached [${i}]: ${tmpPath}`);
      downloadedLocalPaths.push(tmpPath);
      resolved[i] = toLiquidsoapPath(tmpPath);
      await updatePlaylistIfRunning();
      return;
    } catch { /* not cached */ }

    console.log(`[${id}] Downloading [${i}]: ${file}`);

    await downloadFileProgressive(file, tmpPath, id, () => {});

    downloadedLocalPaths.push(tmpPath);
    resolved[i] = toLiquidsoapPath(tmpPath);
    console.log(`[${id}] Complete [${i}]: ${tmpPath}`);
    await updatePlaylistIfRunning();
  });

  await firstFilePromise;

  Promise.all(restPromises)
    .then(() => console.log(`[${id}] All files downloaded and playlist finalized.`))
    .catch((err) => console.error(`[${id}] Background download error: ${err.message}`));

  return downloadedLocalPaths;
}

// ─── Stream Start ─────────────────────────────────────────────────────────────

async function startStream(payload) {
  const id = payload.id || Math.random().toString(36).slice(2, 10);
  if (jobs.has(id) && jobs.get(id).process.exitCode === null) {
    throw new Error(`Stream '${id}' is already running.`);
  }

  const config = {
    id,
    files: Array.isArray(payload.files) ? payload.files : [],
    url: payload.url,
    stream_key: payload.stream_key,
    copy_mode: payload.copy_mode,
    normalize_for_copy: payload.normalize_for_copy === true,
  };

  if (config.files.length === 0) {
    throw new Error("`files` array is required and cannot be empty.");
  }

  const liqPath = path.join(SCRIPTS_DIR, `${id}.liq`);
  const logPath = path.join(LOGS_DIR, `${id}.log`);
  const playlistPath = path.join(PLAYLISTS_DIR, `${id}.txt`);

  await fsp.mkdir(path.dirname(playlistPath), { recursive: true });

  const downloadedFiles = await resolveFilesProgressive(
    id,
    config.files,
    playlistPath,
    async (initialFiles) => {
      let resolvedFiles = initialFiles;

      if (config.copy_mode === true && config.normalize_for_copy) {
        const localPaths = resolvedFiles.map((f) => toOsPath(f));
        const normalized = await normalizeFilesForCopy(id, localPaths);
        resolvedFiles = normalized.map((f) => toLiquidsoapPath(f));
      }

      await writePlaylistFile(playlistPath, resolvedFiles);
      const script = makeScript(config, playlistPath);
      await fsp.writeFile(liqPath, script, "utf8");

      const logStream = fs.createWriteStream(logPath, { flags: "a" });

      function writeLog(msg) {
        const line = `[${new Date().toISOString()}] ${msg}\n`;
        process.stdout.write(line);
        logStream.write(line);
      }

      writeLog(`Starting stream id=${id} (progressive)`);
      writeLog(`Script: ${liqPath}`);
      writeLog(`Playlist: ${playlistPath}`);
      writeLog(`Initial files: ${JSON.stringify(resolvedFiles)}`);

      const child = spawn(LIQUIDSOAP_BIN, [liqPath], {
        cwd: ROOT_DIR,
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });

      writeLog(`Spawned PID=${child.pid}`);

      function pipeLines(stream, label) {
        let buf = "";
        stream.on("data", (chunk) => {
          buf += chunk.toString();
          const lines = buf.split("\n");
          buf = lines.pop();
          for (const line of lines) {
            if (line.trim()) writeLog(`[${label}] ${line}`);
          }
        });
        stream.on("end", () => {
          if (buf.trim()) writeLog(`[${label}] ${buf}`);
        });
      }

      pipeLines(child.stdout, "stdout");
      pipeLines(child.stderr, "stderr");

      child.on("error", (err) => {
        writeLog(`[error] Failed to spawn liquidsoap: ${err.message}`);
        logStream.end();
      });

      child.on("exit", (code, signal) => {
        writeLog(`[exit] code=${code} signal=${signal}`);
        logStream.end();
      });

      jobs.set(id, {
        id,
        process: child,
        liqPath,
        logPath,
        playlistPath,
        downloadedFiles: [],
        createdAt: Date.now(),
        config,
      });
    }
  );

  if (jobs.has(id)) {
    jobs.get(id).downloadedFiles = downloadedFiles;
  }

  return jobSummary(jobs.get(id));
}

// ─── Stream Stop ──────────────────────────────────────────────────────────────

async function waitForExit(child, timeoutMs = 7000) {
  if (child.exitCode !== null) return;
  await Promise.race([
    new Promise((resolve) => child.once("exit", resolve)),
    new Promise((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
}

async function deleteFileSafe(filePath) {
  try {
    await fsp.unlink(filePath);
    console.log(`[cleanup] Deleted: ${filePath}`);
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.warn(`[cleanup] Could not delete ${filePath}: ${err.message}`);
    }
  }
}

async function stopStream(id) {
  const job = jobs.get(id);
  if (!job) throw new Error(`Stream '${id}' not found.`);

  // Process kill
  if (job.process.exitCode === null) {
    if (process.platform === "win32") {
      await new Promise((resolve) => {
        const killer = spawn(
          "taskkill",
          ["/pid", String(job.process.pid), "/t", "/f"],
          { windowsHide: true, stdio: "ignore" }
        );
        killer.on("close", resolve);
      });
      await waitForExit(job.process);
    } else {
      job.process.kill("SIGTERM");
      await waitForExit(job.process);
    }
  }

  // Cleanup — .liq, playlist.txt, log file, downloaded videos
  await Promise.all([
    deleteFileSafe(job.liqPath),
    deleteFileSafe(job.playlistPath),
    deleteFileSafe(job.logPath),
    ...job.downloadedFiles.map((f) => deleteFileSafe(f)),
  ]);

  const summary = jobSummary(job);
  jobs.delete(id);
  return summary;
}

// ─── Playlist Update ──────────────────────────────────────────────────────────

async function updatePlaylist(id, files, applyMode = "after_current") {
  if (!Array.isArray(files) || files.length === 0) {
    throw new Error("`files` array is required and cannot be empty.");
  }

  const job = jobs.get(id);
  if (!job) throw new Error(`Stream '${id}' not found.`);

  let nextFiles = files;
  if (job.config.copy_mode === true && job.config.normalize_for_copy) {
    nextFiles = await normalizeFilesForCopy(id, files);
  }

  await writePlaylistFile(job.playlistPath, nextFiles);
  job.config.files = nextFiles;

  if (applyMode === "after_current") {
    return { ...jobSummary(job), apply_mode: "after_current" };
  }

  if (applyMode === "immediate") {
    const nextConfig = { ...job.config, id, files: nextFiles };
    await stopStream(id);
    const restarted = await startStream(nextConfig);
    return { ...restarted, apply_mode: "immediate" };
  }

  throw new Error("Invalid apply_mode. Use `after_current` or `immediate`.");
}

// ─── HTTP API ─────────────────────────────────────────────────────────────────

app.register(cors, { origin: true });

// API Key auth — সব route এ mandatory
app.addHook("onRequest", async (request, reply) => {
  // /health route auth ছাড়াই accessible রাখো (uptime check এর জন্য)
  if (request.url === "/health") return;

  const provided = request.headers["x-api-key"];

  if (!provided) {
    return reply.code(401).send({ error: "unauthorized", message: "Unauthorized, you cannot access this resource." });
  }

  // Timing-safe comparison — brute force থেকে সুরক্ষা
  const keyBuf = Buffer.from(API_KEY);
  const providedBuf = Buffer.from(provided);

  if (
    keyBuf.length !== providedBuf.length ||
    !crypto.timingSafeEqual(keyBuf, providedBuf)
  ) {
    return reply.code(401).send({ error: "unauthorized", message: "Invalid API key." });
  }
});

app.get("/health", async () => ({ ok: true }));

app.get("/streams", async () => ({
  streams: Array.from(jobs.values()).map(jobSummary),
}));

app.get("/streams/:id/logs", async (request, reply) => {
  const { id } = request.params;
  const job = jobs.get(id);
  if (!job) return reply.code(404).send({ error: "stream not found" });
  const logs = await readLastLines(job.logPath, 150);
  return { id, logs };
});

app.post("/streams/start", async (request, reply) => {
  try {
    const stream = await startStream(request.body || {});
    return reply.code(201).send({ stream });
  } catch (error) {
    return reply.code(400).send({ error: error.message });
  }
});

app.post("/streams/:id/stop", async (request, reply) => {
  try {
    const stream = await stopStream(request.params.id);
    return { stream };
  } catch (error) {
    return reply.code(400).send({ error: error.message });
  }
});

app.post("/streams/:id/playlist", async (request, reply) => {
  try {
    const { files, apply_mode } = request.body || {};
    const stream = await updatePlaylist(
      request.params.id,
      files,
      apply_mode || "after_current"
    );
    return { stream };
  } catch (error) {
    return reply.code(400).send({ error: error.message });
  }
});

// ─── Boot ─────────────────────────────────────────────────────────────────────

async function boot() {
  ensureDirs();
  await app.listen({ host: HOST, port: PORT });
  app.log.info(`stream controller listening on http://${HOST}:${PORT}`);
  app.log.info(`liquidsoap binary: ${LIQUIDSOAP_BIN}`);
  app.log.info(`API key auth: enabled`);
}

boot().catch((err) => {
  app.log.error(err);
  process.exit(1);
});