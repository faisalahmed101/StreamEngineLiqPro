const path = require("node:path");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const { spawn } = require("node:child_process");
const Fastify = require("fastify");
const cors = require("@fastify/cors");

const app = Fastify({ logger: true });

const ROOT_DIR = __dirname;
const RUNTIME_DIR = path.join(ROOT_DIR, "runtime");
const SCRIPTS_DIR = path.join(RUNTIME_DIR, "scripts");
const LOGS_DIR = path.join(RUNTIME_DIR, "logs");
const PLAYLISTS_DIR = path.join(RUNTIME_DIR, "playlists");
const NORMALIZED_DIR = path.join(RUNTIME_DIR, "normalized");

const LIQUIDSOAP_BIN = process.env.LIQUIDSOAP_BIN || "liquidsoap";
const HOST = process.env.STREAM_API_HOST || "127.0.0.1";
const PORT = Number(process.env.STREAM_API_PORT || 8090);
const API_KEY = process.env.STREAM_API_KEY || "";

/** @type {Map<string, {id: string, process: import("node:child_process").ChildProcessWithoutNullStreams, liqPath: string, logPath: string, playlistPath: string, createdAt: number, config: {id?: string, files: string[], url?: string, stream_key?: string, copy_mode?: boolean}}>} */
const jobs = new Map();

function toLiquidsoapPath(inputPath) {
  return String(inputPath).replaceAll("\\", "/");
}

function ensureDirs() {
  for (const dir of [RUNTIME_DIR, SCRIPTS_DIR, LOGS_DIR, PLAYLISTS_DIR, NORMALIZED_DIR]) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }
}

function makeScript(payload, playlistPath) {
  let url = payload.url;
  const streamKey = payload.stream_key;
  if (!url && streamKey) {
    url = `rtmp://a.rtmp.youtube.com/live2/${streamKey}`;
  }
  if (!url) {
    throw new Error("Provide either `url` or `stream_key`.");
  }

  // Safer default for SaaS workloads: transcode unless explicitly requested.
  const copyMode = payload.copy_mode === true;
  const avLine = copyMode
    ? "%video.copy, %audio.copy"
    : "%video(codec=\"libx264\"), %audio(codec=\"aac\")";
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
    if (err && err.code === "ENOENT") return "";
    throw err;
  }
}

async function writePlaylistFile(playlistPath, files) {
  if (!Array.isArray(files) || files.length === 0) {
    throw new Error("`files` array is required and cannot be empty.");
  }
  const normalized = files.map((item) => toLiquidsoapPath(item));
  await fsp.writeFile(playlistPath, `${normalized.join("\n")}\n`, "utf8");
}

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

  if (config.copy_mode === true && config.normalize_for_copy) {
    config.files = await normalizeFilesForCopy(id, config.files);
  }

  const liqPath = path.join(SCRIPTS_DIR, `${id}.liq`);
  const logPath = path.join(LOGS_DIR, `${id}.log`);
  const playlistPath = path.join(PLAYLISTS_DIR, `${id}.txt`);
  await writePlaylistFile(playlistPath, config.files);
  const script = makeScript(config, playlistPath);

  await fsp.writeFile(liqPath, script, "utf8");
  const logStream = fs.createWriteStream(logPath, { flags: "a" });

  const child = spawn(LIQUIDSOAP_BIN, [liqPath], {
    cwd: ROOT_DIR,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });

  child.stdout.pipe(logStream);
  child.stderr.pipe(logStream);
  child.on("exit", () => {
    logStream.end();
  });

  const job = { id, process: child, liqPath, logPath, playlistPath, createdAt: Date.now(), config };
  
  jobs.set(id, job);
  return jobSummary(job);
}

async function waitForExit(child, timeoutMs = 7000) {
  if (child.exitCode !== null) return;
  await Promise.race([
    new Promise((resolve) => child.once("exit", resolve)),
    new Promise((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
}

async function stopStream(id, options = {}) {
  const { remove = false } = options;
  const job = jobs.get(id);
  if (!job) {
    throw new Error(`Stream '${id}' not found.`);
  }

  if (job.process.exitCode === null) {
    if (process.platform === "win32") {
      await new Promise((resolve) => {
        const killer = spawn("taskkill", ["/pid", String(job.process.pid), "/t", "/f"], {
          windowsHide: true,
          stdio: "ignore",
        });
        killer.on("close", resolve);
      });
      await waitForExit(job.process);
    } else {
      job.process.kill("SIGTERM");
      await waitForExit(job.process);
    }
  }

  if (remove) {
    jobs.delete(id);
  }

  return jobSummary(job);
}

async function updatePlaylist(id, files, applyMode = "after_current") {
  if (!Array.isArray(files) || files.length === 0) {
    throw new Error("`files` array is required and cannot be empty.");
  }

  const job = jobs.get(id);
  if (!job) {
    throw new Error(`Stream '${id}' not found.`);
  }

  let nextFiles = files;
  if (job.config.copy_mode === true && job.config.normalize_for_copy) {
    nextFiles = await normalizeFilesForCopy(id, files);
  }

  await writePlaylistFile(job.playlistPath, nextFiles);
  job.config.files = nextFiles;

  // after_current: let current track finish naturally.
  if (applyMode === "after_current") {
    return { ...jobSummary(job), apply_mode: "after_current" };
  }

  // immediate: quick restart so new playlist starts right away.
  if (applyMode === "immediate") {
    const nextConfig = {
      ...job.config,
      id,
      files: nextFiles,
    };
    await stopStream(id, { remove: true });
    const restarted = await startStream(nextConfig);
    return { ...restarted, apply_mode: "immediate" };
  }

  throw new Error("Invalid apply_mode. Use `after_current` or `immediate`.");
}

app.register(cors, { origin: true });

app.addHook("onRequest", async (request, reply) => {
  if (!API_KEY) return;
  const provided = request.headers["x-api-key"];
  if (provided !== API_KEY) {
    reply.code(401).send({ error: "unauthorized" });
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
    const stream = await updatePlaylist(request.params.id, files, apply_mode || "after_current");
    return { stream };
  } catch (error) {
    return reply.code(400).send({ error: error.message });
  }
});

async function boot() {
  ensureDirs();
  await app.listen({ host: HOST, port: PORT });
  app.log.info(`stream controller listening on http://${HOST}:${PORT}`);
  app.log.info(`liquidsoap binary: ${LIQUIDSOAP_BIN}`);
}

boot().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
