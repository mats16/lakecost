import { WorkspaceServiceError } from './workspaceClientErrors.js';

export type DataSourceSetupStep = 'systemGrants' | 'lakeflowJob';

export class DataSourceSetupError extends WorkspaceServiceError {
  readonly step: DataSourceSetupStep | undefined;

  constructor(message: string, statusCode: number, step?: DataSourceSetupStep) {
    super(message, statusCode);
    this.step = step;
  }
}
