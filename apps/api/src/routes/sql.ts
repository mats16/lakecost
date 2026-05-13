import { Router, type Request, type RequestHandler, type Response } from 'express';
import { z } from 'zod';
import {
  SqlStatementResultResponseSchema,
  SqlStatementSubmitRequestSchema,
  SqlStatementSubmitResponseSchema,
  type Env,
} from '@finlake/shared';
import { buildUserExecutor, type StatementExecutor } from '../services/statementExecution.js';

const StatementIdSchema = z.string().min(1).max(256);

const WRITE_KEYWORDS = [
  'ALTER',
  'CREATE',
  'COPY',
  'DELETE',
  'DROP',
  'GRANT',
  'INSERT',
  'MERGE',
  'OPTIMIZE',
  'REPLACE',
  'REVOKE',
  'TRUNCATE',
  'UPDATE',
  'VACUUM',
];

type ExecutorFactory = (
  env: Env,
  token: string | undefined,
  warehouseId?: string,
) => StatementExecutor | undefined;

export function sqlRouter(env: Env, buildExecutor: ExecutorFactory = buildUserExecutor): Router {
  const router = Router();
  router.post('/', submitSqlHandler(env, buildExecutor));
  router.get('/:statement_id', getSqlHandler(env, buildExecutor));
  return router;
}

function submitSqlHandler(env: Env, buildExecutor: ExecutorFactory): RequestHandler {
  return async (req, res, next) => {
    try {
      const parsed = SqlStatementSubmitRequestSchema.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const validationError = validateReadOnlySql(parsed.data.query);
      if (validationError) {
        res.status(400).json({ error: { message: validationError } });
        return;
      }

      const executor = userExecutorFromRequest(
        env,
        req,
        res,
        buildExecutor,
        parsed.data.warehouse_id,
      );
      if (!executor) return;
      const submitted = await executor.submitRaw(
        parsed.data.query,
        parsed.data.params,
        parsed.data.warehouse_id,
      );
      res.json(
        SqlStatementSubmitResponseSchema.parse({
          ...submitted,
          generatedAt: new Date().toISOString(),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

function getSqlHandler(env: Env, buildExecutor: ExecutorFactory): RequestHandler {
  return async (req, res, next) => {
    try {
      const parsed = StatementIdSchema.safeParse(req.params.statement_id);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid statement_id' } });
        return;
      }
      const executor = userExecutorFromRequest(env, req, res, buildExecutor);
      if (!executor) return;
      const result = await executor.getRaw(parsed.data);
      res.json(
        SqlStatementResultResponseSchema.parse({
          ...result,
          generatedAt: new Date().toISOString(),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

function userExecutorFromRequest(
  env: Env,
  req: Request,
  res: Response,
  buildExecutor: ExecutorFactory,
  warehouseId?: string,
) {
  const token = req.user?.accessToken;
  if (!token) {
    res.status(401).json({ error: { message: 'Missing OBO access token' } });
    return undefined;
  }
  const executor = buildExecutor(env, token, warehouseId);
  if (!executor) {
    res
      .status(500)
      .json({ error: { message: 'DATABRICKS_HOST or SQL_WAREHOUSE_ID not configured' } });
    return undefined;
  }
  return executor;
}

export function validateReadOnlySql(sql: string): string | undefined {
  const analysis = analyzeSql(sql);
  const stripped = analysis.stripped.trim();
  if (!stripped) return 'SQL statement is empty';

  if (analysis.statementTerminators.length > 1) {
    return 'Only a single SQL statement is allowed';
  }
  const terminator = analysis.statementTerminators[0];
  if (terminator !== undefined && analysis.stripped.slice(terminator + 1).trim().length > 0) {
    return 'Only a single SQL statement is allowed';
  }

  const firstToken = stripped.match(/^[A-Za-z_][A-Za-z0-9_]*/)?.[0]?.toUpperCase();
  if (firstToken !== 'SELECT' && firstToken !== 'WITH') {
    return 'Only SELECT or WITH statements are allowed';
  }

  const writeKeyword = WRITE_KEYWORDS.find((keyword) =>
    new RegExp(`\\b${keyword}\\b`, 'i').test(analysis.stripped),
  );
  if (writeKeyword) {
    return `Read-only SQL cannot contain ${writeKeyword}`;
  }
  return undefined;
}

function analyzeSql(sql: string): { stripped: string; statementTerminators: number[] } {
  let stripped = '';
  const statementTerminators: number[] = [];
  let i = 0;
  let state:
    | 'normal'
    | 'singleQuote'
    | 'doubleQuote'
    | 'backtick'
    | 'lineComment'
    | 'blockComment' = 'normal';

  while (i < sql.length) {
    const char = sql[i] ?? '';
    const next = sql[i + 1] ?? '';

    if (state === 'lineComment') {
      if (char === '\n') {
        stripped += '\n';
        state = 'normal';
      } else {
        stripped += ' ';
      }
      i += 1;
      continue;
    }

    if (state === 'blockComment') {
      if (char === '*' && next === '/') {
        stripped += '  ';
        state = 'normal';
        i += 2;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'singleQuote') {
      if (char === '\\' && next) {
        stripped += '  ';
        i += 2;
      } else if (char === "'" && next === "'") {
        stripped += '  ';
        i += 2;
      } else if (char === "'") {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'doubleQuote') {
      if (char === '"' && next === '"') {
        stripped += '  ';
        i += 2;
      } else if (char === '"') {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (state === 'backtick') {
      if (char === '`' && next === '`') {
        stripped += '  ';
        i += 2;
      } else if (char === '`') {
        stripped += ' ';
        state = 'normal';
        i += 1;
      } else {
        stripped += char === '\n' ? '\n' : ' ';
        i += 1;
      }
      continue;
    }

    if (char === '-' && next === '-') {
      stripped += '  ';
      state = 'lineComment';
      i += 2;
      continue;
    }
    if (char === '/' && next === '*') {
      stripped += '  ';
      state = 'blockComment';
      i += 2;
      continue;
    }
    if (char === "'") {
      stripped += ' ';
      state = 'singleQuote';
      i += 1;
      continue;
    }
    if (char === '"') {
      stripped += ' ';
      state = 'doubleQuote';
      i += 1;
      continue;
    }
    if (char === '`') {
      stripped += ' ';
      state = 'backtick';
      i += 1;
      continue;
    }
    if (char === ';') {
      statementTerminators.push(stripped.length);
    }
    stripped += char;
    i += 1;
  }

  return { stripped, statementTerminators };
}
