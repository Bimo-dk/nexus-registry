import { Router, type Request, type Response } from 'express';
import { getCachedSnapshot, runHealthCheckCycle } from '../system-health.js';

export const systemRouter = Router();

/**
 * GET /api/system/health
 *   Returnerer seneste sundheds-snapshot (cached, refreshes hvert 30. sek af loop'en).
 *   Brug ?fresh=true for at trigge en ny check med det samme.
 */
systemRouter.get('/health', async (req: Request, res: Response) => {
  if (req.query['fresh'] === 'true') {
    const snapshot = await runHealthCheckCycle();
    res.json(snapshot);
    return;
  }
  const cached = getCachedSnapshot();
  if (cached) {
    res.json(cached);
    return;
  }
  // Ingen cache endnu — kør én nu
  const snapshot = await runHealthCheckCycle();
  res.json(snapshot);
});
