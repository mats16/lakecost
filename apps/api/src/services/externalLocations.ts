import type { Env, ExternalLocationSummary } from '@lakecost/shared';
import { logger } from '../config/logger.js';
import { buildUserWorkspaceClient } from './statementExecution.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

interface ExternalLocationInfoLike {
  name?: string;
  url?: string;
  credential_name?: string;
  read_only?: boolean;
  comment?: string;
}

export class ExternalLocationServiceError extends WorkspaceServiceError {}

export async function listAccessibleExternalLocations(
  env: Env,
  userToken: string | undefined,
): Promise<ExternalLocationSummary[]> {
  if (!userToken) throw new ExternalLocationServiceError('OBO access token required', 401);
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new ExternalLocationServiceError('DATABRICKS_HOST not configured', 500);

  const collected: ExternalLocationSummary[] = [];
  try {
    for await (const item of wc.externalLocations.list({
      include_browse: true,
      max_results: 0, // 0 = use paginated mode per SDK docs
    })) {
      const loc = item as ExternalLocationInfoLike;
      if (!loc.name) continue;
      collected.push({
        name: loc.name,
        url: loc.url ?? null,
        credentialName: loc.credential_name ?? null,
        readOnly: loc.read_only ?? null,
        comment: loc.comment ?? null,
      });
    }
  } catch (err) {
    logger.error({ err }, 'wc.externalLocations.list failed');
    throw new ExternalLocationServiceError(
      `Failed to list external locations: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }

  collected.sort((a, b) => a.name.localeCompare(b.name));
  return collected;
}
