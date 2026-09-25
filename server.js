
const axios = require('axios');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// ============================================================
// CONFIGURATION
// ============================================================

const QBIT_URL = process.env.QBIT_URL;
const QBIT_USERNAME = process.env.QBIT_USERNAME;
const QBIT_PASSWORD = process.env.QBIT_PASSWORD;

const MONGO_URI = process.env.MONGO_URI;
const MONGO_DB = process.env.MONGO_DB;
const MONGO_COLLECTION =
  process.env.MONGO_COLLECTION || 'torrent_monitor';

// Check qBittorrent every 30 seconds
const CHECK_INTERVAL =
  Number(process.env.CHECK_INTERVAL_SECONDS || 30) * 1000;

// Delete only after 20 continuous minutes
const FAILURE_THRESHOLD =
  Number(process.env.FAILURE_THRESHOLD_MINUTES || 20) * 60 * 1000;

// HTTP timeout
const HTTP_TIMEOUT =
  Number(process.env.HTTP_TIMEOUT_SECONDS || 15) * 1000;

// A torrent must have no useful activity for this long
// before a stalled torrent can be considered dead.
const STALLED_THRESHOLD =
  Number(process.env.STALLED_THRESHOLD_MINUTES || 20) * 60 * 1000;

// ============================================================
// VALIDATION
// ============================================================

function requireEnv(name) {
  if (!process.env[name]) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
}

[
  'QBIT_URL',
  'QBIT_USERNAME',
  'QBIT_PASSWORD',
  'MONGO_URI',
  'MONGO_DB',
].forEach(requireEnv);

// ============================================================
// GLOBALS
// ============================================================

let mongoClient;
let collection;

let qbitLoggedIn = false;
let monitorRunning = false;

// ============================================================
// LOGGING
// ============================================================

function timestamp() {
  return new Date().toISOString();
}

function log(level, message, data = null) {
  const prefix = `[${timestamp()}] [${level}]`;

  if (data) {
    console.log(prefix, message, data);
  } else {
    console.log(prefix, message);
  }
}

function info(message, data) {
  log('INFO', message, data);
}

function warn(message, data) {
  log('WARN', message, data);
}

function error(message, data) {
  log('ERROR', message, data);
}

function success(message, data) {
  log('OK', message, data);
}

// ============================================================
// QBITTORRENT CLIENT
// ============================================================

const qbit = axios.create({
  baseURL: QBIT_URL,
  timeout: HTTP_TIMEOUT,
  validateStatus: status => status >= 200 && status < 500,
});

// ============================================================
// QBITTORRENT LOGIN
// ============================================================

async function loginQbit() {
  try {
    const response = await qbit.post(
      '/api/v2/auth/login',
      new URLSearchParams({
        username: QBIT_USERNAME,
        password: QBIT_PASSWORD,
      }),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
      }
    );

    if (response.status !== 200 || response.data !== 'Ok.') {
      throw new Error(
        `Login failed: HTTP ${response.status} ${response.data}`
      );
    }

    qbitLoggedIn = true;

    success('Connected to qBittorrent');

    return true;
  } catch (err) {
    qbitLoggedIn = false;

    error(
      'qBittorrent login failed',
      err.response?.data || err.message
    );

    return false;
  }
}

// ============================================================
// ENSURE LOGIN
// ============================================================

async function ensureQbitLogin() {
  if (!qbitLoggedIn) {
    return await loginQbit();
  }

  return true;
}

// ============================================================
// GET ALL TORRENTS
// ============================================================

async function getTorrents() {
  if (!(await ensureQbitLogin())) {
    throw new Error('qBittorrent login unavailable');
  }

  const response = await qbit.get('/api/v2/torrents/info');

  if (response.status === 403) {
    qbitLoggedIn = false;

    throw new Error('qBittorrent session expired');
  }

  if (response.status !== 200) {
    throw new Error(
      `Failed to get torrents: HTTP ${response.status}`
    );
  }

  return response.data;
}

// ============================================================
// GET TRACKERS
// ============================================================

async function getTrackers(hash) {
  try {
    const response = await qbit.get(
      '/api/v2/torrents/trackers',
      {
        params: {
          hash,
        },
      }
    );

    if (response.status !== 200) {
      return [];
    }

    return Array.isArray(response.data)
      ? response.data
      : [];
  } catch (err) {
    warn(`Unable to retrieve trackers for ${hash}`, err.message);

    return [];
  }
}

// ============================================================
// TRACKER ANALYSIS
// ============================================================

function analyzeTrackers(trackers) {
  const usableTrackers = trackers.filter(
    tracker =>
      tracker.url &&
      !tracker.url.startsWith('**')
  );

  const working = usableTrackers.filter(
    tracker =>
      tracker.status === 2 ||
      tracker.status === 3
  );

  const failed = usableTrackers.filter(
    tracker => tracker.status === 4
  );

  const notContacted = usableTrackers.filter(
    tracker => tracker.status === 1
  );

  return {
    total: usableTrackers.length,
    working: working.length,
    failed: failed.length,
    notContacted: notContacted.length,

    allFailed:
      usableTrackers.length > 0 &&
      failed.length === usableTrackers.length,

    hasWorking:
      working.length > 0,

    hasTrackers:
      usableTrackers.length > 0,

    trackersWithoutPeers:
      usableTrackers.filter(
        tracker =>
          Number(tracker.num_peers || 0) === 0
      ).length,
  };
}

// ============================================================
// TORRENT SNAPSHOT
// ============================================================

function torrentSnapshot(torrent, trackerInfo) {
  return {
    hash: torrent.hash,
    name: torrent.name,
    state: torrent.state,

    progress: Number(torrent.progress || 0),

    dlspeed: Number(torrent.dlspeed || 0),

    dlspeedKB:
      Number(torrent.dlspeed || 0) / 1024,

    numSeeds: Number(torrent.num_seeds || 0),

    numLeechs: Number(torrent.num_leechs || 0),

    numPeers: Number(torrent.num_peers || 0),

    numComplete: Number(torrent.num_complete || 0),

    numIncomplete: Number(torrent.num_incomplete || 0),

    eta: torrent.eta,

    lastActivity:
      torrent.last_activity
        ? new Date(
            Number(torrent.last_activity) * 1000
          )
        : null,

    trackerInfo,
  };
}

// ============================================================
// HARD FAILURE DETECTION
// ============================================================

function getHardFailureReason(torrent) {
  switch (torrent.state) {
    case 'metaDL':
      return 'metadata_download_failed';

    case 'erroredDL':
      return 'download_error';

    default:
      return null;
  }
}

// ============================================================
// STALLED / DEAD TORRENT ANALYSIS
// ============================================================

function analyzeStalledTorrent(torrent, trackerInfo) {
  // Not stalled at all
  if (torrent.state !== 'stalledDL') {
    return {
      stalled: false,
      dead: false,
      reasons: [],
    };
  }

  const reasons = [];

  const downloadSpeed =
    Number(torrent.dlspeed || 0);

  const peers =
    Number(torrent.num_peers || 0);

  const seeds =
    Number(torrent.num_seeds || 0);

  const complete =
    Number(torrent.num_complete || 0);

  // qBittorrent tracker peer count
  const trackerPeers =
    Number(torrent.num_leechs || 0) +
    Number(torrent.num_seeds || 0);

  if (downloadSpeed === 0) {
    reasons.push('zero_download_speed');
  }

  if (peers === 0) {
    reasons.push('no_connected_peers');
  }

  if (seeds === 0) {
    reasons.push('no_seeds');
  }

  if (complete === 0) {
    reasons.push('no_tracker_seeders');
  }

  if (trackerPeers === 0) {
    reasons.push('no_tracker_peers');
  }

  if (trackerInfo.hasTrackers &&
      trackerInfo.allFailed) {
    reasons.push('all_trackers_failed');
  }

  /*
   * IMPORTANT:
   *
   * We do NOT consider a torrent dead just because:
   *
   *   dlspeed = 0
   *   peers = 0
   *
   * Torrents can temporarily have no peers.
   *
   * The conditions below require several independent
   * signals to agree.
   */

  const noDownload =
    downloadSpeed === 0;

  const noPeers =
    peers === 0;

  const noSeeds =
    seeds === 0;

  const trackerDead =
    trackerInfo.hasTrackers &&
    trackerInfo.allFailed;

  const trackerEmpty =
    trackerInfo.hasTrackers &&
    trackerInfo.total > 0 &&
    trackerInfo.working === 0 &&
    trackerInfo.notContacted === 0;

  /*
   * Conservative dead-torrent determination.
   *
   * Case 1:
   * Tracker infrastructure explicitly reports that
   * all trackers have failed.
   *
   * OR
   *
   * Case 2:
   * No peers + no seeds + no download speed +
   * tracker has no usable peers.
   */

  const dead =
    noDownload &&
    noPeers &&
    noSeeds &&
    (
      trackerDead ||
      trackerEmpty ||
      !trackerInfo.hasTrackers
    );

  return {
    stalled: true,
    dead,
    reasons,
  };
}

// ============================================================
// FORMAT DURATION
// ============================================================

function formatDuration(ms) {
  const totalSeconds =
    Math.floor(ms / 1000);

  const hours =
    Math.floor(totalSeconds / 3600);

  const minutes =
    Math.floor((totalSeconds % 3600) / 60);

  const seconds =
    totalSeconds % 60;

  return `${hours}h ${minutes}m ${seconds}s`;
}

// ============================================================
// DELETE TORRENT
// ============================================================

async function deleteTorrent(torrent, reason) {
  info(
    `DELETE REQUEST: ${torrent.name}`,
    {
      hash: torrent.hash,
      reason,
      state: torrent.state,
      progress: torrent.progress,
      dlspeed: torrent.dlspeed,
      peers: torrent.numPeers,
      seeds: torrent.numSeeds,
    }
  );

  try {
    if (!(await ensureQbitLogin())) {
      throw new Error(
        'Cannot delete because qBittorrent login failed'
      );
    }

    const response = await qbit.post(
      '/api/v2/torrents/delete',
      new URLSearchParams({
        hashes: torrent.hash,
        deleteFiles: 'true',
      }),
      {
        headers: {
          'Content-Type':
            'application/x-www-form-urlencoded',
        },
      }
    );

    if (response.status === 403) {
      qbitLoggedIn = false;

      throw new Error(
        'qBittorrent session expired during deletion'
      );
    }

    if (response.status !== 200) {
      throw new Error(
        `Delete failed: HTTP ${response.status}`
      );
    }

    success(
      `DELETED torrent and files: ${torrent.name}`,
      {
        hash: torrent.hash,
        reason,
      }
    );

    // Store deletion history instead of immediately
    // losing the information.
    await collection.updateOne(
      { hash: torrent.hash },
      {
        $set: {
          deleted: true,
          deletedAt: new Date(),
          deletionReason: reason,
        },
      }
    );

    return true;

  } catch (err) {
    error(
      `FAILED TO DELETE: ${torrent.name}`,
      {
        hash: torrent.hash,
        reason,
        error:
          err.response?.data ||
          err.message,
      }
    );

    // IMPORTANT:
    // Do not remove the MongoDB document.
    // This allows the next monitoring cycle to retry.
    return false;
  }
}

// ============================================================
// RESET HEALTHY TORRENT
// ============================================================

async function markHealthy(torrent) {
  const existing =
    await collection.findOne({
      hash: torrent.hash,
      deleted: { $ne: true },
    });

  if (!existing) {
    return;
  }

  info(
    `RECOVERED: ${torrent.name}`,
    {
      hash: torrent.hash,
      previousReason: existing.reason,
      previousBadDuration:
        existing.badSince
          ? formatDuration(
              Date.now() -
              new Date(existing.badSince).getTime()
            )
          : null,
    }
  );

  await collection.deleteOne({
    hash: torrent.hash,
  });
}

// ============================================================
// PROCESS HARD FAILURE
// ============================================================

async function processHardFailure(
  torrent,
  reason,
  trackerInfo
) {
  const now = Date.now();

  let record =
    await collection.findOne({
      hash: torrent.hash,
    });

  // ----------------------------------------------------------
  // FIRST OBSERVATION
  // ----------------------------------------------------------

  if (!record) {
    await collection.insertOne({
      hash: torrent.hash,
      name: torrent.name,

      reason,
      state: torrent.state,

      badSince: new Date(now),
      lastChecked: new Date(now),

      firstProgress:
        Number(torrent.progress || 0),

      lastProgress:
        Number(torrent.progress || 0),

      lastDlspeed:
        Number(torrent.dlspeed || 0),

      createdAt: new Date(),

      deleted: false,
    });

    warn(
      `FAILURE STARTED: ${torrent.name}`,
      {
        hash: torrent.hash,
        reason,
        state: torrent.state,
        progress: torrent.progress,
        dlspeedKB:
          Number(torrent.dlspeed || 0) / 1024,
        peers: torrent.num_peers,
        seeds: torrent.num_seeds,
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // UPDATE RECORD
  // ----------------------------------------------------------

  await collection.updateOne(
    { hash: torrent.hash },
    {
      $set: {
        name: torrent.name,
        reason,
        state: torrent.state,

        lastChecked: new Date(now),

        lastProgress:
          Number(torrent.progress || 0),

        lastDlspeed:
          Number(torrent.dlspeed || 0),
      },
    }
  );

  const badSince =
    new Date(record.badSince).getTime();

  const duration =
    now - badSince;

  const remaining =
    Math.max(
      0,
      FAILURE_THRESHOLD - duration
    );

  info(
    `FAILED TORRENT: ${torrent.name}`,
    {
      hash: torrent.hash,
      reason,
      state: torrent.state,

      badFor:
        formatDuration(duration),

      deleteIn:
        formatDuration(remaining),

      progress:
        torrent.progress,

      dlspeedKB:
        Number(torrent.dlspeed || 0) / 1024,

      peers:
        torrent.num_peers,

      seeds:
        torrent.num_seeds,

      trackers:
        trackerInfo,
    }
  );

  // ----------------------------------------------------------
  // DELETE
  // ----------------------------------------------------------

  if (duration >= FAILURE_THRESHOLD) {
    await deleteTorrent(
      torrent,
      reason
    );
  }
}

// ============================================================
// PROCESS STALLED TORRENT
// ============================================================

async function processStalledTorrent(
  torrent,
  trackerInfo,
  stalledAnalysis
) {
  if (!stalledAnalysis.dead) {

    /*
     * A stalled torrent is NOT automatically bad.
     *
     * We remove any previous stalled/dead timer because
     * the evidence does not currently indicate a dead torrent.
     */

    const existing =
      await collection.findOne({
        hash: torrent.hash,
      });

    if (
      existing &&
      existing.reason === 'dead_stalled_download'
    ) {
      info(
        `STALLED TORRENT RECOVERED / NOT DEAD: ${torrent.name}`,
        {
          hash: torrent.hash,
          previousBadDuration:
            formatDuration(
              Date.now() -
              new Date(existing.badSince).getTime()
            ),
        }
      );

      await collection.deleteOne({
        hash: torrent.hash,
      });
    }

    info(
      `TEMPORARILY STALLED: ${torrent.name}`,
      {
        hash: torrent.hash,

        speedKB:
          Number(torrent.dlspeed || 0) / 1024,

        peers:
          torrent.num_peers,

        seeds:
          torrent.num_seeds,

        trackerPeers:
          Number(torrent.num_leechs || 0),

        trackerStatus:
          trackerInfo,

        reasons:
          stalledAnalysis.reasons,

        decision:
          'KEEP - insufficient evidence of dead torrent',
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // GENUINELY DEAD STALLED TORRENT
  // ----------------------------------------------------------

  const reason =
    'dead_stalled_download';

  await processHardFailure(
    torrent,
    reason,
    trackerInfo
  );
}

// ============================================================
// PROCESS TORRENT
// ============================================================

async function processTorrent(torrent) {
  // ----------------------------------------------------------
  // Get tracker information
  // ----------------------------------------------------------

  const trackers =
    await getTrackers(torrent.hash);

  const trackerInfo =
    analyzeTrackers(trackers);

  // ----------------------------------------------------------
  // Log current torrent
  // ----------------------------------------------------------

  info(
    `TORRENT`,
    {
      name: torrent.name,
      hash: torrent.hash,

      state: torrent.state,

      progress:
        `${(
          Number(torrent.progress || 0) * 100
        ).toFixed(2)}%`,

      downloadSpeed:
        `${(
          Number(torrent.dlspeed || 0) / 1024
        ).toFixed(1)} KB/s`,

      uploadSpeed:
        `${(
          Number(torrent.upspeed || 0) / 1024
        ).toFixed(1)} KB/s`,

      peers:
        torrent.num_peers,

      seeds:
        torrent.num_seeds,

      leechers:
        torrent.num_leechs,

      eta:
        torrent.eta,

      trackers:
        trackerInfo,
    }
  );

  // ----------------------------------------------------------
  // HARD FAILURE
  // ----------------------------------------------------------

  const hardFailure =
    getHardFailureReason(torrent);

  if (hardFailure) {
    await processHardFailure(
      torrent,
      hardFailure,
      trackerInfo
    );

    return;
  }

  // ----------------------------------------------------------
  // STALLED DOWNLOAD
  // ----------------------------------------------------------

  if (torrent.state === 'stalledDL') {
    const stalledAnalysis =
      analyzeStalledTorrent(
        torrent,
        trackerInfo
      );

    await processStalledTorrent(
      torrent,
      trackerInfo,
      stalledAnalysis
    );

    return;
  }

  // ----------------------------------------------------------
  // EVERYTHING ELSE IS HEALTHY / NOT FAILURE
  // ----------------------------------------------------------

  await markHealthy(torrent);
}

// ============================================================
// CLEAN STALE DATABASE RECORDS
// ============================================================

async function cleanupStaleRecords(torrents) {
  const hashes =
    new Set(
      torrents.map(t => t.hash)
    );

  const records =
    await collection.find({
      deleted: { $ne: true },
    }).toArray();

  for (const record of records) {
    if (!hashes.has(record.hash)) {
      info(
        `Removing stale MongoDB record`,
        {
          hash: record.hash,
          name: record.name,
          reason: record.reason,
        }
      );

      await collection.deleteOne({
        _id: record._id,
      });
    }
  }
}

// ============================================================
// MAIN MONITOR
// ============================================================

async function monitor() {
  if (monitorRunning) {
    warn(
      'Previous monitor cycle is still running - skipping'
    );

    return;
  }

  monitorRunning = true;

  const cycleStart =
    Date.now();

  try {
    info('================================================');
    info('STARTING MONITOR CYCLE');

    const torrents =
      await getTorrents();

    info(
      `Found ${torrents.length} torrents`
    );

    for (const torrent of torrents) {
      try {
        await processTorrent(torrent);
      } catch (err) {
        error(
          `Failed processing torrent: ${torrent.name}`,
          err.message
        );
      }
    }

    await cleanupStaleRecords(
      torrents
    );

    info(
      `MONITOR CYCLE COMPLETE`,
      {
        torrents: torrents.length,
        duration:
          `${Date.now() - cycleStart}ms`,
      }
    );

  } catch (err) {
    error(
      'MONITOR CYCLE FAILED',
      err.response?.data ||
      err.message
    );

    // Force re-login next cycle
    qbitLoggedIn = false;

  } finally {
    monitorRunning = false;
  }
}

// ============================================================
// MONGODB
// ============================================================

async function connectMongo() {
  mongoClient =
    new MongoClient(
      MONGO_URI,
      {
        serverSelectionTimeoutMS: 10000,
      }
    );

  await mongoClient.connect();

  const db =
    mongoClient.db(MONGO_DB);

  collection =
    db.collection(
      MONGO_COLLECTION
    );

  await collection.createIndex(
    { hash: 1 },
    { unique: true }
  );

  await collection.createIndex(
    { badSince: 1 }
  );

  await collection.createIndex(
    { deleted: 1 }
  );

  success(
    `MongoDB connected: ${MONGO_DB}.${MONGO_COLLECTION}`
  );
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function shutdown(signal) {
  info(
    `Received ${signal}. Shutting down...`
  );

  try {
    if (mongoClient) {
      await mongoClient.close();
    }

    success('MongoDB connection closed');

  } catch (err) {
    error(
      'Error during shutdown',
      err.message
    );
  }

  process.exit(0);
}

process.on(
  'SIGINT',
  () => shutdown('SIGINT')
);

process.on(
  'SIGTERM',
  () => shutdown('SIGTERM')
);

// ============================================================
// START
// ============================================================

async function start() {
  console.log('');
  console.log('==============================================');
  console.log('       qBittorrent Torrent Monitor');
  console.log('==============================================');
  console.log('');

  info(
    'Configuration',
    {
      qbitURL: QBIT_URL,
      mongoDB: MONGO_DB,
      collection: MONGO_COLLECTION,

      checkInterval:
        `${CHECK_INTERVAL / 1000}s`,

      failureThreshold:
        `${FAILURE_THRESHOLD / 60000} minutes`,

      stalledThreshold:
        `${STALLED_THRESHOLD / 60000} minutes`,
    }
  );

  try {
    await connectMongo();

    const loggedIn =
      await loginQbit();

    if (!loggedIn) {
      throw new Error(
        'Unable to connect to qBittorrent'
      );
    }

    // Immediate first check
    await monitor();

    // Continuous monitoring
    setInterval(
      monitor,
      CHECK_INTERVAL
    );

    success(
      `Monitor started. Checking every ${
        CHECK_INTERVAL / 1000
      } seconds.`
    );

  } catch (err) {
    error(
      'FATAL STARTUP ERROR',
      err.message
    );

    process.exit(1);
  }
}

start();

