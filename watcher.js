
import "dotenv/config";
import chokidar from "chokidar";
import fs from "node:fs/promises";
import path from "node:path";
import sharp from "sharp";

const TOKEN = process.env.DISCORD_TOKEN;
const API = "https://discord.com/api/v10";

const config = JSON.parse(
  await fs.readFile("./config.json", "utf8")
);

const GUILD_ID = config.guildId;
const ROOT_DIR = path.resolve(config.rootDir);
const CHANNELS_FILE = path.resolve("./channels.json");
const QUEUE_FILE = path.resolve("./upload-queue.json");

const MAX_FILE_SIZE = 10 * 1024 * 1024;
const RETRY_DELAY = 15_000;

const IMAGE_TYPES = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".bmp": "image/bmp",
  ".avif": "image/avif",
  ".tif": "image/tiff",
  ".tiff": "image/tiff"
};

if (!TOKEN || !GUILD_ID || !config.rootDir) {
  throw new Error(
    "Please configure DISCORD_TOKEN and config.json."
  );
}

await fs.access(ROOT_DIR);

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

let channelMap = {};
let uploadQueue = [];
let queueRunning = false;
let queueSaveChain = Promise.resolve();

const channelPromises = new Map();
const inFlight = new Set();

let guildChannelsCache = null;
let requestChain = Promise.resolve();

async function loadJson(filePath, fallback) {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return fallback;
    throw error;
  }
}

channelMap = await loadJson(CHANNELS_FILE, {});

const savedQueue = await loadJson(QUEUE_FILE, []);

if (!Array.isArray(savedQueue)) {
  throw new Error("upload-queue.json must contain an array.");
}

// Migrate queue entries saved by the earlier string-based version.
uploadQueue = savedQueue.map(item =>
  typeof item === "string"
    ? {
        path: path.resolve(item),
        imageMessageId: null,
        captionSent: false
      }
    : {
        path: path.resolve(item.path),
        imageMessageId: item.imageMessageId ?? null,
        captionSent: item.captionSent === true
      }
);

function saveJsonAtomic(filePath, data) {
  const operation = async () => {
    const tempFile = `${filePath}.tmp`;

    await fs.writeFile(
      tempFile,
      JSON.stringify(data, null, 2),
      "utf8"
    );

    await fs.rename(tempFile, filePath);
  };

  const result = queueSaveChain.then(operation, operation);
  queueSaveChain = result.catch(() => {});

  return result;
}

async function saveChannels() {
  await saveJsonAtomic(CHANNELS_FILE, channelMap);
}

async function saveQueue() {
  await saveJsonAtomic(QUEUE_FILE, uploadQueue);
}

// All Discord REST requests share one serialized queue.
// A 429 pauses the queue for the server-provided duration.
function discordRequest(endpoint, options = {}) {
  const run = async () => {
    while (true) {
      let response;

      try {
        response = await fetch(`${API}${endpoint}`, {
          ...options,
          headers: {
            Authorization: `Bot ${TOKEN}`,
            ...options.headers
          }
        });
      } catch (error) {
        throw error;
      }

      if (response.status === 429) {
        let data = {};

        try {
          data = await response.json();
        } catch {
          // Use a conservative fallback if no valid body exists.
        }

        const retryAfter = Number(data.retry_after);

        const waitMs =
          Number.isFinite(retryAfter) && retryAfter >= 0
            ? retryAfter * 1000 + 250
            : 2000;

        console.warn(
          `Rate limited${data.global ? " globally" : ""}. ` +
          `Retrying in ${(waitMs / 1000).toFixed(1)} seconds.`
        );

        await sleep(waitMs);
        continue;
      }

      if (response.status >= 500) {
        console.warn(
          `Discord returned ${response.status} for ${endpoint}.`
        );

        throw new Error(`Discord server error ${response.status}`);
      }

      if (!response.ok) {
        const details = await response.text();

        throw new Error(
          `Discord API ${response.status}: ${details}`
        );
      }

      if (response.status === 204) return null;

      return response.json();
    }
  };

  const result = requestChain.then(run, run);
  requestChain = result.catch(() => {});

  return result;
}

async function refreshGuildChannels() {
  guildChannelsCache = await discordRequest(
    `/guilds/${GUILD_ID}/channels`
  );

  return guildChannelsCache;
}

function addCachedChannel(channel) {
  if (!guildChannelsCache) guildChannelsCache = [];

  const index = guildChannelsCache.findIndex(
    item => item.id === channel.id
  );

  if (index === -1) {
    guildChannelsCache.push(channel);
  } else {
    guildChannelsCache[index] = channel;
  }
}

function updateCachedChannel(channelId, changes) {
  if (!guildChannelsCache) return;

  const channel = guildChannelsCache.find(
    item => item.id === channelId
  );

  if (channel) Object.assign(channel, changes);
}

function folderKey(folderPath) {
  const relative = path.relative(ROOT_DIR, folderPath);

  return relative
    ? relative.split(path.sep).join("/")
    : "_root";
}

function channelName(folder) {
  const slug = folder
    .replaceAll("/", "-")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return (slug || "root").slice(0, 100);
}

function categoryNumber(name) {
  if (name === "Disk2Discord") return 1;

  const match = name.match(/^Disk2Discord([2-9]|[1-9]\d+)$/);

  return match ? Number(match[1]) : null;
}

async function ensureCategory() {
  const channels = guildChannelsCache ?? await refreshGuildChannels();

  const categories = channels
    .filter(channel =>
      channel.type === 4 &&
      categoryNumber(channel.name) !== null
    )
    .sort(
      (a, b) =>
        categoryNumber(a.name) - categoryNumber(b.name)
    );

  for (const category of categories) {
    const children = channels.filter(
      channel => channel.parent_id === category.id
    );

    if (children.length < 50) {
      return category.id;
    }
  }

  const usedNumbers = new Set(
    categories.map(category => categoryNumber(category.name))
  );

  let nextNumber = 1;

  while (usedNumbers.has(nextNumber)) nextNumber++;

  const name =
    nextNumber === 1
      ? "Disk2Discord"
      : `Disk2Discord${nextNumber}`;

  const created = await discordRequest(
    `/guilds/${GUILD_ID}/channels`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        name,
        type: 4
      })
    }
  );

  addCachedChannel(created);

  console.log(`Created category: ${name}`);

  return created.id;
}

async function moveChannelToCategory(channelId, categoryId) {
  const updated = await discordRequest(
    `/channels/${channelId}`,
    {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        parent_id: categoryId
      })
    }
  );

  if (updated) addCachedChannel(updated);

  updateCachedChannel(channelId, {
    parent_id: categoryId
  });
}

async function ensureChannel(folder) {
  if (channelPromises.has(folder)) {
    return channelPromises.get(folder);
  }

  const promise = (async () => {
    const marker = `Disk2Discord folder: ${folder}`;
    const channels = guildChannelsCache ?? await refreshGuildChannels();

    let channel = channels.find(
      item => item.type === 0 && item.topic === marker
    );

    if (!channel && channelMap[folder]) {
      channel = channels.find(
        item => item.id === channelMap[folder]
      );
    }

    if (channel) {
      const parent = channels.find(
        item => item.id === channel.parent_id
      );

      if (
        !parent ||
        parent.type !== 4 ||
        categoryNumber(parent.name) === null
      ) {
        const categoryId = await ensureCategory();

        await moveChannelToCategory(channel.id, categoryId);

        console.log(
          `Moved "${channel.name}" into a Disk2Discord category.`
        );
      }

      channelMap[folder] = channel.id;
      await saveChannels();

      return channel.id;
    }

    const categoryId = await ensureCategory();

    const created = await discordRequest(
      `/guilds/${GUILD_ID}/channels`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          name: channelName(folder),
          type: 0,
          topic: marker,
          parent_id: categoryId
        })
      }
    );

    addCachedChannel(created);

    channelMap[folder] = created.id;
    await saveChannels();

    console.log(
      `Created channel "${created.name}" in a Disk2Discord category.`
    );

    return created.id;
  })();

  channelPromises.set(folder, promise);

  try {
    return await promise;
  } catch (error) {
    channelPromises.delete(folder);
    throw error;
  }
}

async function prepareImage(filePath, originalBuffer) {
  if (originalBuffer.length <= MAX_FILE_SIZE) {
    return {
      buffer: originalBuffer,
      name: path.basename(filePath),
      mimeType: IMAGE_TYPES[path.extname(filePath).toLowerCase()]
    };
  }

  if (path.extname(filePath).toLowerCase() === ".gif") {
    throw new Error(
      "GIF exceeds 10 MiB. Original animation preserved."
    );
  }

  console.log(
    `Optimizing image larger than 10 MiB: ${path.basename(filePath)}`
  );

  const outputName =
    `${path.basename(filePath, path.extname(filePath))}.webp`;

  for (const quality of [85, 75, 65, 55, 45, 35]) {
    const buffer = await sharp(originalBuffer, {
      limitInputPixels: 100_000_000
    })
      .rotate()
      .webp({ quality, effort: 5 })
      .toBuffer();

    if (buffer.length <= MAX_FILE_SIZE) {
      return {
        buffer,
        name: outputName,
        mimeType: "image/webp"
      };
    }
  }

  for (const dimension of [4096, 3072, 2048, 1536]) {
    const buffer = await sharp(originalBuffer, {
      limitInputPixels: 100_000_000
    })
      .rotate()
      .resize({
        width: dimension,
        height: dimension,
        fit: "inside",
        withoutEnlargement: true
      })
      .webp({ quality: 35, effort: 6 })
      .toBuffer();

    if (buffer.length <= MAX_FILE_SIZE) {
      return {
        buffer,
        name: outputName,
        mimeType: "image/webp"
      };
    }
  }

  throw new Error(
    "Could not reduce image below 10 MiB. Original preserved."
  );
}

async function processQueueItem(job) {
  const filePath = job.path;

  try {
    if (!job.imageMessageId) {
      let info;

      try {
        info = await fs.stat(filePath);
      } catch (error) {
        if (error.code === "ENOENT") {
          console.warn(`File no longer exists: ${filePath}`);
          return true;
        }

        throw error;
      }

      if (!info.isFile()) return true;

      const extension = path.extname(filePath).toLowerCase();

      if (!IMAGE_TYPES[extension]) return true;

      const originalBuffer = await fs.readFile(filePath);
      const currentInfo = await fs.stat(filePath);

      if (
        currentInfo.size !== originalBuffer.length ||
        currentInfo.mtimeMs !== info.mtimeMs
      ) {
        console.log(`File changed during read; retrying later: ${filePath}`);
        return false;
      }

      const prepared = await prepareImage(filePath, originalBuffer);

      const folder = folderKey(path.dirname(filePath));
      const channelId = await ensureChannel(folder);

      const form = new FormData();

      form.append(
        "files[0]",
        new Blob([prepared.buffer], {
          type: prepared.mimeType
        }),
        prepared.name
      );

      const imageMessage = await discordRequest(
        `/channels/${channelId}/messages`,
        {
          method: "POST",
          body: form
        }
      );

      if (!imageMessage?.id) {
        throw new Error("Discord did not confirm the image message.");
      }

      // Save the image message ID before attempting its caption.
      // This lets future retries continue with the caption.
      job.imageMessageId = imageMessage.id;
      await saveQueue();
    }

    if (!job.captionSent) {
      const folderPath = path.dirname(filePath);
      const gameName = path.basename(folderPath) || "Root";
      const fileName = path.basename(filePath);

      const caption = `-# [${fileName}]`;

      await discordRequest(
        `/channels/${channelMap[folderKey(folderPath)]}/messages`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            content: caption.slice(0, 2000),
            /*message_reference: {
              message_id: job.imageMessageId
            },*/
            allowed_mentions: {
              parse: []
            }
          })
        }
      );

      job.captionSent = true;
      await saveQueue();
    }

    try {
      await fs.unlink(filePath);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }

    console.log(`Uploaded successfully: ${filePath}`);

    return true;
  } catch (error) {
    console.error(
      `Upload failed; file preserved where possible: ${filePath}`,
      error.message
    );

    return false;
  }
}

async function processUploadQueue() {
  if (queueRunning) return;

  queueRunning = true;

  try {
    while (uploadQueue.length > 0) {
      const job = uploadQueue[0];

      console.log(
        `Processing queue (${uploadQueue.length} remaining): ${job.path}`
      );

      const success = await processQueueItem(job);

      if (success) {
        uploadQueue.shift();
        await saveQueue();
        continue;
      }

      console.warn(
        `Retrying failed job in ${RETRY_DELAY / 1000} seconds.`
      );

      await sleep(RETRY_DELAY);
    }
  } catch (error) {
    console.error("Upload queue error:", error.message);
  } finally {
    queueRunning = false;
  }
}

async function enqueueImage(filePath) {
  const absolutePath = path.resolve(filePath);

  if (
    !uploadQueue.some(job => job.path === absolutePath) &&
    !inFlight.has(absolutePath)
  ) {
    uploadQueue.push({
      path: absolutePath,
      imageMessageId: null,
      captionSent: false
    });

    await saveQueue();
  }

  processUploadQueue();
}

async function scanDirectories(dir) {
  const entries = await fs.readdir(dir, {
    withFileTypes: true
  });

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;

    const fullPath = path.join(dir, entry.name);

    try {
      await ensureChannel(folderKey(fullPath));
      await scanDirectories(fullPath);
    } catch (error) {
      console.error(
        `Could not prepare folder ${fullPath}:`,
        error.message
      );

      // Folder preparation will be retried on the next startup.
    }
  }
}

async function scanExistingImages(dir, found = []) {
  const entries = await fs.readdir(dir, {
    withFileTypes: true
  });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      await scanExistingImages(fullPath, found);
      continue;
    }

    if (
      entry.isFile() &&
      IMAGE_TYPES[path.extname(entry.name).toLowerCase()]
    ) {
      found.push(path.resolve(fullPath));
    }
  }

  return found;
}

console.log(`Watching: ${ROOT_DIR}`);

await refreshGuildChannels();

const watcher = chokidar.watch(ROOT_DIR, {
  ignoreInitial: true,
  persistent: true,
  awaitWriteFinish: {
    stabilityThreshold: 1500,
    pollInterval: 100
  }
});

watcher.on("addDir", dir => {
  if (path.resolve(dir) === ROOT_DIR) return;

  ensureChannel(folderKey(dir)).catch(error => {
    console.error(
      `Could not prepare folder ${dir}:`,
      error.message
    );
  });
});

watcher.on("add", filePath => {
  if (!IMAGE_TYPES[path.extname(filePath).toLowerCase()]) {
    return;
  }

  enqueueImage(filePath).catch(error => {
    console.error("Could not enqueue image:", error.message);
  });
});

watcher.on("error", error => {
  console.error("Watcher error:", error);
});

// Wait until watching is active before scanning. This avoids
// missing files created while the startup scan is running.
await new Promise((resolve, reject) => {
  watcher.once("ready", resolve);
  watcher.once("error", reject);
});

await scanDirectories(ROOT_DIR);

const existingImages = await scanExistingImages(ROOT_DIR);
const queuedPaths = new Set(uploadQueue.map(job => job.path));

for (const filePath of existingImages) {
  if (!queuedPaths.has(filePath)) {
    uploadQueue.push({
      path: filePath,
      imageMessageId: null,
      captionSent: false
    });

    queuedPaths.add(filePath);
  }
}

await saveQueue();

console.log(
  `Queued ${uploadQueue.length} images for upload.`
);

console.log("Disk2Discord is running.");

processUploadQueue();
