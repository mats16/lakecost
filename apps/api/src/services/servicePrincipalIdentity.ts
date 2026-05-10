import type { Env } from '@lakecost/shared';
import { logger } from '../config/logger.js';
import { buildAppWorkspaceClient, type WorkspaceClient } from './statementExecution.js';
import { type WorkspaceServiceError, isPermissionDenied } from './workspaceClientErrors.js';

type ServiceErrorCtor = new (message: string, statusCode: number) => WorkspaceServiceError;

interface CurrentPrincipalLike {
  id?: string;
  userName?: string;
  displayName?: string;
  externalId?: string;
}

export async function currentServicePrincipalOwnerAliases(
  wc: WorkspaceClient,
  env: Env,
): Promise<Set<string>> {
  const aliases = new Set<string>();
  addOwnerAlias(aliases, env.DATABRICKS_CLIENT_ID);

  const current = (await wc.currentUser.me()) as CurrentPrincipalLike;
  addOwnerAlias(aliases, current.id);
  addOwnerAlias(aliases, current.userName);
  addOwnerAlias(aliases, current.displayName);
  addOwnerAlias(aliases, current.externalId);

  return aliases;
}

export function requireAppWorkspaceClient(env: Env, ErrorClass: ServiceErrorCtor): WorkspaceClient {
  const wc = buildAppWorkspaceClient(env);
  if (!wc) {
    throw new ErrorClass('Databricks app service principal credentials not configured', 500);
  }
  return wc;
}

export async function resolveOwnerAliases(
  wc: WorkspaceClient,
  env: Env,
  ErrorClass: ServiceErrorCtor,
): Promise<Set<string>> {
  try {
    return await currentServicePrincipalOwnerAliases(wc, env);
  } catch (err) {
    logger.error({ err }, 'wc.currentUser.me failed');
    throw new ErrorClass(
      `Failed to resolve app service principal identity: ${(err as Error).message}`,
      isPermissionDenied(err) ? 403 : 502,
    );
  }
}

export function isOwnedByCurrentServicePrincipal(
  owner: string | undefined,
  aliases: Set<string>,
): boolean {
  const normalized = normalizeOwner(owner);
  return Boolean(normalized && aliases.has(normalized));
}

function addOwnerAlias(aliases: Set<string>, value: string | undefined): void {
  const normalized = normalizeOwner(value);
  if (normalized) aliases.add(normalized);
}

function normalizeOwner(value: string | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed.toLocaleLowerCase() : null;
}
