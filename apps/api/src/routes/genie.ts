import { Router, type Response } from 'express';
import type { DatabaseClient } from '@finlake/db';
import { GenieChatRequestSchema, type Env } from '@finlake/shared';
import {
  GenieServiceError,
  askFinLakeGenie,
  deleteFinLakeGenieSpace,
  setupFinLakeGenieSpace,
  streamFinLakeGenieConversation,
  streamFinLakeGenieExistingMessage,
  streamFinLakeGenieMessage,
  type GenieStreamEvent,
} from '../services/genie.js';

export function genieRouter(db: DatabaseClient, env: Env): Router {
  const router = Router();

  router.post('/setup', async (req, res, next) => {
    try {
      const result = await setupFinLakeGenieSpace(env, db);
      res.json(result);
    } catch (err) {
      if (err instanceof GenieServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.delete('/space', async (_req, res, next) => {
    try {
      await deleteFinLakeGenieSpace(env, db);
      res.status(204).end();
    } catch (err) {
      if (err instanceof GenieServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.post('/chat', async (req, res, next) => {
    try {
      const parsed = GenieChatRequestSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res
          .status(400)
          .json({ error: { message: parsed.error.issues[0]?.message ?? 'Invalid request' } });
        return;
      }
      const result = await askFinLakeGenie(env, db, {
        ...parsed.data,
        userAccessToken: req.user?.accessToken,
      });
      res.json(result);
    } catch (err) {
      if (err instanceof GenieServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.post('/:alias/messages', async (req, res, next) => {
    try {
      if (!isFinLakeAlias(req.params.alias)) {
        res
          .status(404)
          .json({ error: { message: `Unknown Genie space alias: ${req.params.alias}` } });
        return;
      }
      const parsed = GenieChatRequestSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res
          .status(400)
          .json({ error: { message: parsed.error.issues[0]?.message ?? 'Invalid request' } });
        return;
      }
      prepareSse(res);
      await streamFinLakeGenieMessage(env, db, {
        ...parsed.data,
        userAccessToken: req.user?.accessToken,
        emit: (event) => writeSse(res, event),
      });
      res.end();
    } catch (err) {
      handleSseError(err, res, next);
    }
  });

  router.get('/:alias/conversations/:conversationId', async (req, res, next) => {
    try {
      if (!isFinLakeAlias(req.params.alias)) {
        res
          .status(404)
          .json({ error: { message: `Unknown Genie space alias: ${req.params.alias}` } });
        return;
      }
      prepareSse(res);
      await streamFinLakeGenieConversation(env, db, {
        conversationId: req.params.conversationId,
        pageToken: typeof req.query.pageToken === 'string' ? req.query.pageToken : undefined,
        includeQueryResults: req.query.includeQueryResults !== 'false',
        userAccessToken: req.user?.accessToken,
        emit: (event) => writeSse(res, event),
      });
      res.end();
    } catch (err) {
      handleSseError(err, res, next);
    }
  });

  router.get(
    '/:alias/conversations/:conversationId/messages/:messageId',
    async (req, res, next) => {
      try {
        if (!isFinLakeAlias(req.params.alias)) {
          res
            .status(404)
            .json({ error: { message: `Unknown Genie space alias: ${req.params.alias}` } });
          return;
        }
        prepareSse(res);
        await streamFinLakeGenieExistingMessage(env, db, {
          conversationId: req.params.conversationId,
          messageId: req.params.messageId,
          userAccessToken: req.user?.accessToken,
          emit: (event) => writeSse(res, event),
        });
        res.end();
      } catch (err) {
        handleSseError(err, res, next);
      }
    },
  );

  return router;
}

function isFinLakeAlias(alias: string | undefined): boolean {
  return alias === 'finlake' || alias === 'default';
}

function prepareSse(res: Response): void {
  res.setHeader('content-type', 'text/event-stream');
  res.setHeader('cache-control', 'no-cache, no-transform');
  res.setHeader('connection', 'keep-alive');
  res.flushHeaders?.();
}

function writeSse(res: Response, event: GenieStreamEvent): void {
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

function handleSseError(err: unknown, res: Response, next: (err: unknown) => void): void {
  if (res.headersSent) {
    writeSse(res, {
      type: 'error',
      error: err instanceof Error ? err.message : 'Genie request failed',
    });
    res.end();
    return;
  }
  if (err instanceof GenieServiceError) {
    res.status(err.statusCode).json({ error: { message: err.message } });
    return;
  }
  next(err);
}
