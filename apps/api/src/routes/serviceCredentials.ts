import { Router } from 'express';
import {
  ServiceCredentialCreateBodySchema,
  StorageCredentialCreateBodySchema,
  AwsFocusExportCreateBodySchema,
  type Env,
} from '@lakecost/shared';
import {
  AwsFocusExportServiceError,
  createAwsFocusExportResources,
} from '../services/awsFocusExport.js';
import {
  createAwsServiceCredential,
  deleteCredential,
  listAccessibleServiceCredentials,
  ServiceCredentialServiceError,
} from '../services/serviceCredentials.js';
import {
  createAwsStorageCredential,
  listServicePrincipalStorageCredentials,
  StorageCredentialServiceError,
} from '../services/storageCredentials.js';
import { ExternalLocationServiceError } from '../services/externalLocations.js';

export function serviceCredentialsRouter(env: Env): Router {
  const router = Router();

  router.get('/', async (_req, res, next) => {
    try {
      const [storageCredentials, serviceCredentials] = await Promise.all([
        listServicePrincipalStorageCredentials(env),
        listAccessibleServiceCredentials(env),
      ]);
      res.setHeader('Cache-Control', 'no-store');
      res.json({ storageCredentials, serviceCredentials });
    } catch (err) {
      if (
        err instanceof ServiceCredentialServiceError ||
        err instanceof StorageCredentialServiceError
      ) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.post('/', async (req, res, next) => {
    try {
      const registeredBy = req.user?.email ?? req.user?.userName ?? req.user?.userId ?? 'unknown';
      if (req.body?.purpose === 'STORAGE') {
        const parsed = StorageCredentialCreateBodySchema.safeParse(req.body);
        if (!parsed.success) {
          res
            .status(400)
            .json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
          return;
        }
        const storageCredential = await createAwsStorageCredential(env, {
          ...parsed.data,
          comment: `Registered by ${registeredBy} via FinLake`,
        });
        res.status(201).json({ storageCredential });
        return;
      }

      const parsed = ServiceCredentialCreateBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const serviceCredential = await createAwsServiceCredential(env, {
        ...parsed.data,
        comment: `Registered by ${registeredBy} via FinLake`,
      });
      res.status(201).json({ serviceCredential });
    } catch (err) {
      if (
        err instanceof ServiceCredentialServiceError ||
        err instanceof StorageCredentialServiceError
      ) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.post('/aws-focus-export', async (req, res, next) => {
    try {
      const parsed = AwsFocusExportCreateBodySchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: { message: 'Invalid input', issues: parsed.error.issues } });
        return;
      }
      const result = await createAwsFocusExportResources(env, parsed.data);
      res.status(201).json(result);
    } catch (err) {
      if (
        err instanceof AwsFocusExportServiceError ||
        err instanceof ServiceCredentialServiceError ||
        err instanceof StorageCredentialServiceError ||
        err instanceof ExternalLocationServiceError
      ) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  router.delete('/:name', async (req, res, next) => {
    try {
      await deleteCredential(env, req.params.name);
      res.status(204).end();
    } catch (err) {
      if (err instanceof ServiceCredentialServiceError) {
        res.status(err.statusCode).json({ error: { message: err.message } });
        return;
      }
      next(err);
    }
  });

  return router;
}
