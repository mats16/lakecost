import { Buffer } from 'node:buffer';
import type { Env } from '@finlake/shared';
import { logger } from '../config/logger.js';
import { databricksErrorMessage } from '../services/genieClient.js';
import type { ServiceErrorCtor } from '../services/workspaceClientErrors.js';

interface OAuthTokenResponse {
  access_token?: string;
  token_type?: string;
  expires_in?: number;
}

let cachedSpToken: { token: string; expiresAt: number } | null = null;

export async function fetchServicePrincipalToken(
  host: string,
  env: Env,
  ErrorClass: ServiceErrorCtor,
): Promise<string> {
  if (cachedSpToken && Date.now() < cachedSpToken.expiresAt) {
    return cachedSpToken.token;
  }
  const clientId = env.DATABRICKS_CLIENT_ID;
  const clientSecret = env.DATABRICKS_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new ErrorClass('Databricks service principal credentials are not configured.', 500);
  }

  let response: Response;
  try {
    response = await fetch(`${host}/oidc/v1/token`, {
      method: 'POST',
      headers: {
        authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        'content-type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        scope: 'all-apis',
      }).toString(),
    });
  } catch (err) {
    logger.error({ err }, 'Databricks service principal token request failed');
    throw new ErrorClass(
      `Failed to get service principal OAuth token: ${(err as Error).message}`,
      502,
    );
  }

  if (!response.ok) {
    const message = await databricksErrorMessage(response);
    logger.warn({ status: response.status, message }, 'Databricks service principal token failed');
    throw new ErrorClass(
      `Failed to get service principal OAuth token: ${message}`,
      response.status === 401 ? 401 : response.status === 403 ? 403 : 502,
    );
  }

  const body = (await response.json()) as OAuthTokenResponse;
  const token = body.access_token?.trim();
  if (!token) {
    throw new ErrorClass('Databricks service principal token response had no access_token.', 502);
  }
  const expiresIn = body.expires_in ?? 3600;
  cachedSpToken = { token, expiresAt: Date.now() + (expiresIn - 60) * 1000 };
  return token;
}
