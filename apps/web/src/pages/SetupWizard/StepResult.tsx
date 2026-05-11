import type { SetupCheckResult } from '@finlake/shared';
import { Alert, AlertDescription, AlertTitle } from '@databricks/appkit-ui/react';
import { CheckCircle2, AlertCircle, AlertTriangle, HelpCircle } from 'lucide-react';
import { CodeBlock } from '../../components/CodeBlock';
import { useI18n } from '../../i18n';

const ICON_BY_STATUS = {
  ok: CheckCircle2,
  error: AlertCircle,
  warning: AlertTriangle,
  unknown: HelpCircle,
} as const;

const VARIANT_BY_STATUS: Record<
  SetupCheckResult['status'],
  React.ComponentProps<typeof Alert>['variant']
> = {
  ok: 'default',
  error: 'destructive',
  warning: 'default',
  unknown: 'default',
};

export function StepResult({ result }: { result: SetupCheckResult | null | undefined }) {
  if (!result) return null;
  const Icon = ICON_BY_STATUS[result.status] ?? HelpCircle;
  return (
    <div className="mt-4 grid gap-3">
      <Alert variant={VARIANT_BY_STATUS[result.status]}>
        <Icon />
        <AlertTitle>{result.status.toUpperCase()}</AlertTitle>
        <AlertDescription>{result.message}</AlertDescription>
      </Alert>
      {result.remediation ? <Remediation result={result} /> : null}
    </div>
  );
}

function Remediation({ result }: { result: SetupCheckResult }) {
  const { t } = useI18n();
  const r = result.remediation;
  if (!r) return null;
  return (
    <div className="grid gap-3">
      {r.sql ? (
        <RemediationBlock label={t('stepResult.sql')}>
          <CodeBlock>{r.sql}</CodeBlock>
        </RemediationBlock>
      ) : null}
      {r.terraform ? (
        <RemediationBlock label={t('stepResult.terraform')}>
          <CodeBlock>{r.terraform}</CodeBlock>
        </RemediationBlock>
      ) : null}
      {r.cli ? (
        <RemediationBlock label={t('stepResult.cli')}>
          <CodeBlock>{r.cli}</CodeBlock>
        </RemediationBlock>
      ) : null}
      {r.curl ? (
        <RemediationBlock label={t('stepResult.rest')}>
          <CodeBlock>{r.curl}</CodeBlock>
        </RemediationBlock>
      ) : null}
    </div>
  );
}

function RemediationBlock({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <h4 className="text-muted-foreground mt-0 mb-1.5 text-[11px] font-medium tracking-wider uppercase">
        {label}
      </h4>
      {children}
    </div>
  );
}
