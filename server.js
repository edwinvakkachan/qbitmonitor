
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

// Delete after 20 continuous minutes
const FAILURE_THRESHOLD =
  Number(process.env.FAILURE_THRESHOLD_MINUTES || 20) * 60 * 1000;

// Startup grace period.
// During this period NOTHING can be deleted.
const STARTUP_GRACE_PERIOD =
  Number(process.env.STARTUP_GRACE_MINUTES || 10) * 60 * 1000;

// HTTP timeout
const HTTP_TIMEOUT =
  Number(process.env.HTTP_TIMEOUT_SECONDS || 15) * 1000;

// ============================================================
// VALIDATE ENVIRONMENT
// ============================================================

function requireEnv(name) {
  if (!process.env[name]) {
    throw new Error(
      `Missing required environment variable: ${name}`
    );
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
// GLOBAL STATE
// ============================================================

let mongoClient;
let collection;

let qbitLoggedIn = false;
let monitorRunning = false;

// Every time the monitor starts, it gets a NEW session ID.
//
// This is important.
//
// A torrent that was failing before a 4-hour shutdown must
// NOT continue its old failure timer after startup.
const MONITOR_SESSION_ID =
  new Date().toISOString();

const MONITOR_STARTED_AT =
  Date.now();

// ============================================================
// LOGGING
// ============================================================

function timestamp() {
  return new Date().toISOString();
}

function log(level, message, data = null) {
  const prefix =
    `[${timestamp()}] [${level}]`;

  if (data !== null) {
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

  validateStatus:
    status => status >= 200 && status < 500,
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
          'Content-Type':
            'application/x-www-form-urlencoded',
        },
      }
    );

    if (
      response.status !== 200 ||
      response.data !== 'Ok.'
    ) {
      throw new Error(
        `Login failed: HTTP ${response.status} ${response.data}`
      );
    }

    qbitLoggedIn = true;

    success(
      'Successfully connected to qBittorrent'
    );

    return true;

  } catch (err) {

    qbitLoggedIn = false;

    error(
      'qBittorrent login failed',
      err.response?.data ||
      err.message
    );

    return false;
  }
}

// ============================================================
// ENSURE QBIT LOGIN
// ============================================================

async function ensureQbitLogin() {
  if (!qbitLoggedIn) {
    return await loginQbit();
  }

  return true;
}

// ============================================================
// GET TORRENTS
// ============================================================

async function getTorrents() {

  if (!(await ensureQbitLogin())) {
    throw new Error(
      'Unable to login to qBittorrent'
    );
  }

  const response =
    await qbit.get(
      '/api/v2/torrents/info'
    );

  if (response.status === 403) {
    qbitLoggedIn = false;

    throw new Error(
      'qBittorrent session expired'
    );
  }

  if (response.status !== 200) {
    throw new Error(
      `Unable to get torrents: HTTP ${response.status}`
    );
  }

  if (!Array.isArray(response.data)) {
    throw new Error(
      'qBittorrent returned invalid torrent data'
    );
  }

  return response.data;
}

// ============================================================
// GET TRACKERS
// ============================================================

async function getTrackers(hash) {

  try {

    const response =
      await qbit.get(
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

    warn(
      `Unable to get tracker information for ${hash}`,
      err.message
    );

    return [];
  }
}

// ============================================================
// TRACKER ANALYSIS
// ============================================================

function analyzeTrackers(trackers) {

  const usableTrackers =
    trackers.filter(
      tracker =>
        tracker.url &&
        !tracker.url.startsWith('**')
    );

  const working =
    usableTrackers.filter(
      tracker =>
        tracker.status === 2 ||
        tracker.status === 3
    );

  const failed =
    usableTrackers.filter(
      tracker =>
        tracker.status === 4
    );

  const notContacted =
    usableTrackers.filter(
      tracker =>
        tracker.status === 1
    );

  return {

    total:
      usableTrackers.length,

    working:
      working.length,

    failed:
      failed.length,

    notContacted:
      notContacted.length,

    allFailed:
      usableTrackers.length > 0 &&
      failed.length ===
        usableTrackers.length,

    hasWorking:
      working.length > 0,

    hasTrackers:
      usableTrackers.length > 0,

    trackerPeerCount:
      usableTrackers.reduce(
        (total, tracker) =>
          total +
          Number(
            tracker.num_peers || 0
          ),

        0
      ),
  };
}

// ============================================================
// HARD FAILURE DETECTION
//
// IMPORTANT:
// missingFiles IS NOT INCLUDED.
//
// This means missingFiles can NEVER trigger deletion.
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
// STALLED TORRENT ANALYSIS
// ============================================================

function analyzeStalledTorrent(
  torrent,
  trackerInfo
) {

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

  const trackerPeers =
    trackerInfo.trackerPeerCount;

  // ----------------------------------------------------------
  // INDIVIDUAL CONDITIONS
  // ----------------------------------------------------------

  if (downloadSpeed === 0) {
    reasons.push(
      'zero_download_speed'
    );
  }

  if (peers === 0) {
    reasons.push(
      'no_connected_peers'
    );
  }

  if (seeds === 0) {
    reasons.push(
      'no_seeds'
    );
  }

  if (trackerPeers === 0) {
    reasons.push(
      'no_tracker_peers'
    );
  }

  if (
    trackerInfo.hasTrackers &&
    trackerInfo.allFailed
  ) {
    reasons.push(
      'all_trackers_failed'
    );
  }

  // ----------------------------------------------------------
  // DEAD TORRENT DECISION
  //
  // We require multiple signals.
  //
  // This prevents a normal temporary stall from being deleted.
  // ----------------------------------------------------------

  const noDownload =
    downloadSpeed === 0;

  const noPeers =
    peers === 0;

  const noSeeds =
    seeds === 0;

  const noTrackerPeers =
    trackerPeers === 0;

  const trackersDead =
    trackerInfo.hasTrackers &&
    trackerInfo.allFailed;

  /*
   * Case A:
   *
   * All trackers failed AND
   * no peers/seeds/download.
   */

  const deadBecauseTrackers =
    noDownload &&
    noPeers &&
    noSeeds &&
    trackersDead;

  /*
   * Case B:
   *
   * There are no useful tracker peers at all.
   *
   * This also covers torrents with no usable tracker
   * information.
   */

  const deadBecauseNoPeers =
    noDownload &&
    noPeers &&
    noSeeds &&
    noTrackerPeers;

  const dead =
    deadBecauseTrackers ||
    deadBecauseNoPeers;

  return {

    stalled: true,

    dead,

    reasons,

    signals: {
      noDownload,
      noPeers,
      noSeeds,
      noTrackerPeers,
      trackersDead,
    },
  };
}

// ============================================================
// FORMAT DURATION
// ============================================================

function formatDuration(ms) {

  const totalSeconds =
    Math.floor(ms / 1000);

  const hours =
    Math.floor(
      totalSeconds / 3600
    );

  const minutes =
    Math.floor(
      (totalSeconds % 3600) / 60
    );

  const seconds =
    totalSeconds % 60;

  return `${hours}h ${minutes}m ${seconds}s`;
}

// ============================================================
// STARTUP GRACE CHECK
// ============================================================

function isStartupGraceActive() {

  return (
    Date.now() -
    MONITOR_STARTED_AT <
    STARTUP_GRACE_PERIOD
  );
}

function startupGraceRemaining() {

  const elapsed =
    Date.now() -
    MONITOR_STARTED_AT;

  return Math.max(
    0,
    STARTUP_GRACE_PERIOD -
      elapsed
  );
}

// ============================================================
// DELETE TORRENT
// ============================================================

async function deleteTorrent(
  torrent,
  reason
) {

  // ----------------------------------------------------------
  // ABSOLUTE SAFETY CHECK
  // ----------------------------------------------------------

  if (isStartupGraceActive()) {

    warn(
      `DELETION BLOCKED BY STARTUP GRACE: ${torrent.name}`,

      {
        hash: torrent.hash,

        reason,

        remaining:
          formatDuration(
            startupGraceRemaining()
          ),
      }
    );

    return false;
  }

  info(
    `DELETE REQUEST: ${torrent.name}`,

    {
      hash: torrent.hash,

      reason,

      state: torrent.state,

      progress:
        Number(torrent.progress || 0),

      downloadSpeed:
        Number(torrent.dlspeed || 0),

      peers:
        Number(torrent.num_peers || 0),

      seeds:
        Number(torrent.num_seeds || 0),

      deleteFiles: true,
    }
  );

  try {

    if (!(await ensureQbitLogin())) {
      throw new Error(
        'Cannot delete because qBittorrent login failed'
      );
    }

    const response =
      await qbit.post(

        '/api/v2/torrents/delete',

        new URLSearchParams({

          hashes:
            torrent.hash,

          deleteFiles:
            'true',
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
        hash:
          torrent.hash,

        reason,
      }
    );

    // Keep an audit record in MongoDB.
    await collection.updateOne(

      {
        hash:
          torrent.hash,
      },

      {
        $set: {

          deleted:
            true,

          deletedAt:
            new Date(),

          deletionReason:
            reason,

          monitorSession:
            MONITOR_SESSION_ID,
        },
      }
    );

    return true;

  } catch (err) {

    error(
      `FAILED TO DELETE: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        reason,

        error:
          err.response?.data ||
          err.message,
      }
    );

    // DO NOT remove MongoDB record.
    //
    // The next cycle can retry deletion.

    return false;
  }
}

// ============================================================
// PROCESS HARD FAILURE
// ============================================================

async function processHardFailure(
  torrent,
  reason,
  trackerInfo
) {

  const now =
    Date.now();

  // ----------------------------------------------------------
  // Find current-session failure record
  // ----------------------------------------------------------

  let record =
    await collection.findOne({

      hash:
        torrent.hash,

      deleted:
        { $ne: true },
    });

  // ----------------------------------------------------------
  // OLD FAILURE RECORD FROM PREVIOUS SESSION
  // ----------------------------------------------------------

  if (
    record &&
    record.monitorSession !==
      MONITOR_SESSION_ID
  ) {

    info(
      `RESETTING OLD FAILURE TIMER: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        oldSession:
          record.monitorSession,

        newSession:
          MONITOR_SESSION_ID,

        reason,
      }
    );

    await collection.deleteOne({
      _id:
        record._id,
    });

    record = null;
  }

  // ----------------------------------------------------------
  // FIRST OBSERVATION
  // ----------------------------------------------------------

  if (!record) {

    await collection.insertOne({

      hash:
        torrent.hash,

      name:
        torrent.name,

      reason,

      state:
        torrent.state,

      badSince:
        new Date(now),

      lastChecked:
        new Date(now),

      firstProgress:
        Number(
          torrent.progress || 0
        ),

      lastProgress:
        Number(
          torrent.progress || 0
        ),

      lastDlspeed:
        Number(
          torrent.dlspeed || 0
        ),

      monitorSession:
        MONITOR_SESSION_ID,

      createdAt:
        new Date(),

      deleted:
        false,
    });

    warn(
      `FAILURE TIMER STARTED: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        reason,

        state:
          torrent.state,

        progress:
          torrent.progress,

        downloadSpeed:
          `${(
            Number(
              torrent.dlspeed || 0
            ) / 1024
          ).toFixed(1)} KB/s`,

        peers:
          torrent.num_peers,

        seeds:
          torrent.num_seeds,

        trackers:
          trackerInfo,

        deleteAfter:
          `${FAILURE_THRESHOLD / 60000} minutes`,
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // REASON CHANGED
  // ----------------------------------------------------------

  if (
    record.reason !== reason
  ) {

    warn(
      `FAILURE REASON CHANGED: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        oldReason:
          record.reason,

        newReason:
          reason,
      }
    );

    await collection.updateOne(

      {
        _id:
          record._id,
      },

      {
        $set: {

          reason,

          state:
            torrent.state,

          badSince:
            new Date(now),

          lastChecked:
            new Date(now),

          lastProgress:
            Number(
              torrent.progress || 0
            ),

          lastDlspeed:
            Number(
              torrent.dlspeed || 0
            ),
        },
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // CALCULATE FAILURE DURATION
  // ----------------------------------------------------------

  const badSince =
    new Date(
      record.badSince
    ).getTime();

  const duration =
    now - badSince;

  const remaining =
    Math.max(
      0,
      FAILURE_THRESHOLD -
        duration
    );

  // ----------------------------------------------------------
  // UPDATE
  // ----------------------------------------------------------

  await collection.updateOne(

    {
      _id:
        record._id,
    },

    {
      $set: {

        state:
          torrent.state,

        lastChecked:
          new Date(now),

        lastProgress:
          Number(
            torrent.progress || 0
          ),

        lastDlspeed:
          Number(
            torrent.dlspeed || 0
          ),
      },
    }
  );

  info(
    `FAILED TORRENT: ${torrent.name}`,

    {
      hash:
        torrent.hash,

      reason,

      state:
        torrent.state,

      badFor:
        formatDuration(duration),

      deleteIn:
        formatDuration(remaining),

      progress:
        `${(
          Number(
            torrent.progress || 0
          ) * 100
        ).toFixed(2)}%`,

      downloadSpeed:
        `${(
          Number(
            torrent.dlspeed || 0
          ) / 1024
        ).toFixed(1)} KB/s`,

      peers:
        torrent.num_peers,

      seeds:
        torrent.num_seeds,

      trackerInfo,
    }
  );

  // ----------------------------------------------------------
  // STARTUP GRACE
  // ----------------------------------------------------------

  if (isStartupGraceActive()) {

    info(
      `DELETION BLOCKED - STARTUP GRACE ACTIVE: ${torrent.name}`,

      {
        remaining:
          formatDuration(
            startupGraceRemaining()
          ),
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // DELETE
  // ----------------------------------------------------------

  if (
    duration >=
    FAILURE_THRESHOLD
  ) {

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

  // ----------------------------------------------------------
  // NOT ENOUGH EVIDENCE
  // ----------------------------------------------------------

  if (
    !stalledAnalysis.dead
  ) {

    const existing =
      await collection.findOne({

        hash:
          torrent.hash,

        deleted:
          { $ne: true },
      });

    if (
      existing &&
      existing.reason ===
        'dead_stalled_download'
    ) {

      info(
        `STALL RECOVERED: ${torrent.name}`,

        {
          hash:
            torrent.hash,

          previousBadDuration:
            formatDuration(

              Date.now() -
              new Date(
                existing.badSince
              ).getTime()
            ),

          peers:
            torrent.num_peers,

          seeds:
            torrent.num_seeds,

          downloadSpeed:
            `${(
              Number(
                torrent.dlspeed || 0
              ) / 1024
            ).toFixed(1)} KB/s`,
        }
      );

      await collection.deleteOne({
        _id:
          existing._id,
      });
    }

    info(
      `TEMPORARILY STALLED - KEEPING: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        downloadSpeed:
          `${(
            Number(
              torrent.dlspeed || 0
            ) / 1024
          ).toFixed(1)} KB/s`,

        peers:
          torrent.num_peers,

        seeds:
          torrent.num_seeds,

        trackerPeers:
          trackerInfo.trackerPeerCount,

        trackerStatus:
          trackerInfo,

        reasons:
          stalledAnalysis.reasons,

        decision:
          'KEEP',
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // GENUINELY DEAD
  // ----------------------------------------------------------

  await processHardFailure(

    torrent,

    'dead_stalled_download',

    trackerInfo
  );
}

// ============================================================
// MARK HEALTHY
// ============================================================

async function markHealthy(torrent) {

  const existing =
    await collection.findOne({

      hash:
        torrent.hash,

      deleted:
        { $ne: true },
    });

  if (!existing) {
    return;
  }

  info(
    `TORRENT RECOVERED: ${torrent.name}`,

    {
      hash:
        torrent.hash,

      previousReason:
        existing.reason,

      previousBadDuration:
        existing.badSince
          ? formatDuration(
              Date.now() -
              new Date(
                existing.badSince
              ).getTime()
            )
          : null,

      currentState:
        torrent.state,

      currentProgress:
        torrent.progress,
    }
  );

  await collection.deleteOne({
    _id:
      existing._id,
  });
}

// ============================================================
// PROCESS TORRENT
// ============================================================

async function processTorrent(torrent) {

  // ----------------------------------------------------------
  // IMPORTANT:
  //
  // missingFiles is intentionally NOT processed as a failure.
  // ----------------------------------------------------------

  if (
    torrent.state ===
    'missingFiles'
  ) {

    info(
      `MISSING FILES - IGNORED: ${torrent.name}`,

      {
        hash:
          torrent.hash,

        state:
          torrent.state,

        decision:
          'NO DELETE',
      }
    );

    return;
  }

  // ----------------------------------------------------------
  // TRACKER INFORMATION
  // ----------------------------------------------------------

  const trackers =
    await getTrackers(
      torrent.hash
    );

  const trackerInfo =
    analyzeTrackers(
      trackers
    );

  // ----------------------------------------------------------
  // HARD FAILURE
  // ----------------------------------------------------------

  const hardFailure =
    getHardFailureReason(
      torrent
    );

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

  if (
    torrent.state ===
    'stalledDL'
  ) {

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
  // EVERYTHING ELSE
  // ----------------------------------------------------------

  await markHealthy(
    torrent
  );
}

// ============================================================
// CLEAN STALE FAILURE RECORDS
// ============================================================

async function cleanupStaleRecords(
  torrents
) {

  const hashes =
    new Set(
      torrents.map(
        torrent =>
          torrent.hash
      )
    );

  const records =
    await collection
      .find({
        deleted:
          { $ne: true },
      })
      .toArray();

  for (
    const record of records
  ) {

    if (
      !hashes.has(
        record.hash
      )
    ) {

      info(
        'Removing stale MongoDB failure record',

        {
          hash:
            record.hash,

          name:
            record.name,

          reason:
            record.reason,
        }
      );

      await collection.deleteOne({
        _id:
          record._id,
      });
    }
  }
}

// ============================================================
// MONITOR CYCLE
// ============================================================

async function monitor() {

  if (monitorRunning) {

    warn(
      'Previous monitor cycle still running - skipping'
    );

    return;
  }

  monitorRunning = true;

  const cycleStart =
    Date.now();

  try {

    info(
      '================================================'
    );

    info(
      'STARTING MONITOR CYCLE'
    );

    // --------------------------------------------------------
    // STARTUP STATUS
    // --------------------------------------------------------

    if (
      isStartupGraceActive()
    ) {

      info(
        'STARTUP GRACE MODE ACTIVE',

        {
          remaining:
            formatDuration(
              startupGraceRemaining()
            ),

          deletion:
            'DISABLED',
        }
      );
    }

    // --------------------------------------------------------
    // GET TORRENTS
    // --------------------------------------------------------

    const torrents =
      await getTorrents();

    info(
      `Found ${torrents.length} torrents`
    );

    // --------------------------------------------------------
    // PROCESS
    // --------------------------------------------------------

    for (
      const torrent of torrents
    ) {

      try {

        await processTorrent(
          torrent
        );

      } catch (err) {

        error(
          `Error processing torrent: ${torrent.name}`,

          err.message
        );
      }
    }

    // --------------------------------------------------------
    // CLEAN STALE RECORDS
    // --------------------------------------------------------

    await cleanupStaleRecords(
      torrents
    );

    // --------------------------------------------------------
    // COMPLETE
    // --------------------------------------------------------

    info(
      'MONITOR CYCLE COMPLETE',

      {
        torrentCount:
          torrents.length,

        duration:
          `${Date.now() - cycleStart}ms`,

        startupGrace:
          isStartupGraceActive(),
      }
    );

  } catch (err) {

    error(
      'MONITOR CYCLE FAILED',

      err.response?.data ||
      err.message
    );

    /*
     * VERY IMPORTANT:
     *
     * If qBittorrent itself is unavailable,
     * force re-login next cycle.
     *
     * No deletion occurs because processTorrent()
     * was never reached.
     */

    qbitLoggedIn = false;

  } finally {

    monitorRunning =
      false;
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
        serverSelectionTimeoutMS:
          10000,
      }
    );

  await mongoClient.connect();

  const db =
    mongoClient.db(
      MONGO_DB
    );

  collection =
    db.collection(
      MONGO_COLLECTION
    );

  await collection.createIndex(
    {
      hash: 1,
    },

    {
      unique: true,
    }
  );

  await collection.createIndex({
    badSince: 1,
  });

  await collection.createIndex({
    monitorSession: 1,
  });

  await collection.createIndex({
    deleted: 1,
  });

  success(
    `MongoDB connected: ${MONGO_DB}.${MONGO_COLLECTION}`
  );
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function shutdown(
  signal
) {

  info(
    `Received ${signal}. Shutting down...`
  );

  try {

    if (mongoClient) {

      await mongoClient.close();

      success(
        'MongoDB connection closed'
      );
    }

  } catch (err) {

    error(
      'Shutdown error',
      err.message
    );
  }

  process.exit(0);
}

process.on(
  'SIGINT',
  () =>
    shutdown('SIGINT')
);

process.on(
  'SIGTERM',
  () =>
    shutdown('SIGTERM')
);

// ============================================================
// START
// ============================================================

async function start() {

  console.log('');
  console.log(
    '======================================================'
  );
  console.log(
    '          qBittorrent Torrent Monitor'
  );
  console.log(
    '======================================================'
  );
  console.log('');

  info(
    'Monitor session started',

    {
      session:
        MONITOR_SESSION_ID,

      qbitURL:
        QBIT_URL,

      mongoDB:
        MONGO_DB,

      collection:
        MONGO_COLLECTION,

      checkInterval:
        `${CHECK_INTERVAL / 1000}s`,

      failureThreshold:
        `${FAILURE_THRESHOLD / 60000} minutes`,

      startupGrace:
        `${STARTUP_GRACE_PERIOD / 60000} minutes`,

      automaticDeletion:
        'metaDL, erroredDL, genuinely-dead stalledDL',

      missingFiles:
        'IGNORED',
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

    // First check
    await monitor();

    // Continuous monitoring
    setInterval(
      monitor,
      CHECK_INTERVAL
    );

    success(
      `Monitor started - checking every ${
        CHECK_INTERVAL / 1000
      } seconds`
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
