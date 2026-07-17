import prisma from '@/lib/prisma';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const DEFAULT_RETENTION_MONTHS = 26;
const BATCH_SIZE = 10_000;
const MAX_BATCHES = 20;

function retentionMonths() {
  const parsed = Number.parseInt(process.env.UMAMI_RETENTION_MONTHS || '', 10);
  return Number.isFinite(parsed) && parsed >= 1 && parsed <= 60 ? parsed : DEFAULT_RETENTION_MONTHS;
}

function isAuthorized(request: Request) {
  const secret = process.env.CRON_SECRET;
  return Boolean(secret && request.headers.get('authorization') === `Bearer ${secret}`);
}

async function deleteBatches(
  table: string,
  idColumn: string,
  createdColumn: string,
  months: number,
) {
  let deleted = 0;
  for (let batch = 0; batch < MAX_BATCHES; batch += 1) {
    const count = await prisma.client.$executeRawUnsafe(`
      DELETE FROM "${table}"
      WHERE "${idColumn}" IN (
        SELECT "${idColumn}"
        FROM "${table}"
        WHERE "${createdColumn}" < NOW() - INTERVAL '${months} months'
        ORDER BY "${createdColumn}" ASC
        LIMIT ${BATCH_SIZE}
      )
    `);
    deleted += count;
    if (count < BATCH_SIZE) break;
  }
  return deleted;
}

/**
 * Umami self-hosted data is otherwise retained indefinitely. This bounded job enforces the same
 * retention promise as the host platform without long deletes or a full-table lock.
 */
export async function GET(request: Request) {
  if (!isAuthorized(request)) {
    return Response.json({ error: 'unauthorized' }, { status: 401 });
  }
  if (process.env.CLICKHOUSE_URL) {
    return Response.json(
      { error: 'clickhouse_retention_requires_partition_policy' },
      { status: 503 },
    );
  }

  const months = retentionMonths();
  try {
    // Dependent/event-level tables first; Prisma's relationMode does not rely on database cascades.
    const deleted = {
      eventData: await deleteBatches('event_data', 'event_data_id', 'created_at', months),
      revenue: await deleteBatches('revenue', 'revenue_id', 'created_at', months),
      sessionData: await deleteBatches('session_data', 'session_data_id', 'created_at', months),
      heatmapEvents: await deleteBatches('heatmap_event', 'heatmap_event_id', 'created_at', months),
      replayChunks: await deleteBatches('session_replay', 'replay_id', 'created_at', months),
      websiteEvents: await deleteBatches('website_event', 'event_id', 'created_at', months),
    };

    const deletedSessions = await prisma.client.$executeRawUnsafe(`
      DELETE FROM "session" s
      WHERE s."session_id" IN (
        SELECT candidate."session_id"
        FROM "session" candidate
        WHERE candidate."created_at" < NOW() - INTERVAL '${months} months'
          AND NOT EXISTS (SELECT 1 FROM "website_event" e WHERE e."session_id" = candidate."session_id")
          AND NOT EXISTS (SELECT 1 FROM "session_data" d WHERE d."session_id" = candidate."session_id")
          AND NOT EXISTS (SELECT 1 FROM "revenue" r WHERE r."session_id" = candidate."session_id")
        ORDER BY candidate."created_at" ASC
        LIMIT ${BATCH_SIZE}
      )
    `);

    const [health] = await prisma.client.$queryRawUnsafe<
      Array<{
        websites: bigint;
        events24h: bigint;
        oldestEventAt: Date | null;
        expiredRemaining: bigint;
      }>
    >(`
      SELECT
        (SELECT count(*) FROM "website" WHERE "deleted_at" IS NULL) AS "websites",
        (SELECT count(*) FROM "website_event" WHERE "created_at" >= NOW() - INTERVAL '24 hours') AS "events24h",
        (SELECT min("created_at") FROM "website_event") AS "oldestEventAt",
        (SELECT count(*) FROM "website_event" WHERE "created_at" < NOW() - INTERVAL '${months} months') AS "expiredRemaining"
    `);

    const payload = {
      ok: true,
      retentionMonths: months,
      deleted: { ...deleted, sessions: deletedSessions },
      health: {
        websites: Number(health?.websites || 0),
        events24h: Number(health?.events24h || 0),
        oldestEventAt: health?.oldestEventAt || null,
        expiredRemaining: Number(health?.expiredRemaining || 0),
      },
      checkedAt: new Date().toISOString(),
    };

    if (payload.health.expiredRemaining > 0) {
      console.warn('[maintenance] retention backlog remains', JSON.stringify(payload));
    } else {
      console.info('[maintenance] retention complete', JSON.stringify(payload));
    }
    return Response.json(payload, { headers: { 'Cache-Control': 'no-store' } });
  } catch (error) {
    console.error('[maintenance] retention failed', error);
    return Response.json(
      { ok: false, error: 'maintenance_failed' },
      { status: 500, headers: { 'Cache-Control': 'no-store' } },
    );
  }
}
