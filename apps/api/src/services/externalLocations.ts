import type { Env, ExternalLocationCreateBody, ExternalLocationSummary } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { buildUserWorkspaceClient } from './statementExecution.js';
import {
  isOwnedByCurrentServicePrincipal,
  requireAppWorkspaceClient,
  resolveOwnerAliases,
} from './servicePrincipalIdentity.js';
import { WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

interface ExternalLocationInfoLike {
  name?: string;
  url?: string;
  credential_name?: string;
  read_only?: boolean;
  comment?: string;
  owner?: string;
}

export class ExternalLocationServiceError extends WorkspaceServiceError {}

interface ExternalLocationUpdateInput extends ExternalLocationCreateBody {
  currentName: string;
  newName?: string;
}

export async function listAccessibleExternalLocations(
  env: Env,
  userToken: string | undefined,
): Promise<ExternalLocationSummary[]> {
  if (!userToken) throw new ExternalLocationServiceError('OBO access token required', 401);
  const wc = buildUserWorkspaceClient(env, userToken);
  if (!wc) throw new ExternalLocationServiceError('DATABRICKS_HOST not configured', 500);
  return collectExternalLocations(wc);
}

export async function listServicePrincipalExternalLocations(
  env: Env,
): Promise<ExternalLocationSummary[]> {
  const wc = requireAppWorkspaceClient(env, ExternalLocationServiceError);
  const ownerAliases = await resolveOwnerAliases(wc, env, ExternalLocationServiceError);
  return collectExternalLocations(wc, (loc) =>
    isOwnedByCurrentServicePrincipal(loc.owner, ownerAliases),
  );
}

async function collectExternalLocations(
  wc: ReturnType<typeof buildUserWorkspaceClient> & {},
  filter?: (loc: ExternalLocationInfoLike) => boolean,
): Promise<ExternalLocationSummary[]> {
  const collected: ExternalLocationSummary[] = [];
  try {
    for await (const item of wc.externalLocations.list({
      include_browse: true,
      max_results: 0,
    })) {
      const loc = item as ExternalLocationInfoLike;
      if (!loc.name) continue;
      if (filter && !filter(loc)) continue;
      collected.push(toSummary(loc));
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

export async function createExternalLocation(
  env: Env,
  input: ExternalLocationCreateBody,
): Promise<ExternalLocationSummary> {
  const wc = requireAppWorkspaceClient(env, ExternalLocationServiceError);
  try {
    const created = await wc.externalLocations.create({
      name: input.name,
      url: input.url,
      credential_name: input.credentialName,
      read_only: input.readOnly ?? false,
      comment: input.comment?.trim() || 'FinLake external location',
      skip_validation: true,
    });
    return toSummary(created as ExternalLocationInfoLike, input);
  } catch (err) {
    logger.error({ err }, 'wc.externalLocations.create failed');
    throw new ExternalLocationServiceError(
      `Failed to create external location: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

export async function updateExternalLocation(
  env: Env,
  input: ExternalLocationUpdateInput,
): Promise<ExternalLocationSummary> {
  const wc = requireAppWorkspaceClient(env, ExternalLocationServiceError);
  try {
    const updated = await wc.externalLocations.update({
      name: input.currentName,
      ...(input.newName && input.newName !== input.currentName ? { new_name: input.newName } : {}),
      url: input.url,
      credential_name: input.credentialName,
      read_only: input.readOnly ?? false,
      comment: input.comment?.trim() || 'FinLake external location',
      skip_validation: true,
      force: true,
    });
    return toSummary(updated as ExternalLocationInfoLike, {
      ...input,
      name: input.newName ?? input.currentName,
    });
  } catch (err) {
    logger.error({ err }, 'wc.externalLocations.update failed');
    throw new ExternalLocationServiceError(
      `Failed to update external location: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

export async function deleteExternalLocation(env: Env, name: string): Promise<void> {
  const wc = requireAppWorkspaceClient(env, ExternalLocationServiceError);
  try {
    await wc.externalLocations.delete({ name });
  } catch (err) {
    logger.error({ err }, 'wc.externalLocations.delete failed');
    throw new ExternalLocationServiceError(
      `Failed to delete external location: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

function toSummary(
  loc: ExternalLocationInfoLike,
  fallback?: ExternalLocationCreateBody,
): ExternalLocationSummary {
  return {
    name: loc.name ?? fallback?.name ?? '',
    url: loc.url ?? fallback?.url ?? null,
    credentialName: loc.credential_name ?? fallback?.credentialName ?? null,
    readOnly: loc.read_only ?? fallback?.readOnly ?? null,
    comment: loc.comment ?? fallback?.comment ?? null,
  };
}
