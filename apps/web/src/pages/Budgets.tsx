import { useCallback, useEffect, useState, type FormEvent } from 'react';
import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Field,
  FieldGroup,
  FieldLabel,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Spinner,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@databricks/appkit-ui/react';
import { AlertCircle, Plus, X } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { useBudgets, useCreateBudget } from '../api/hooks';
import { BudgetScopeTypeSchema, BudgetPeriodSchema, type CreateBudgetInput } from '@finlake/shared';
import { useCurrencyUsd, useI18n } from '../i18n';

const SCOPE_OPTIONS = BudgetScopeTypeSchema.options;
const PERIOD_OPTIONS = BudgetPeriodSchema.options;

export function Budgets() {
  const { t } = useI18n();
  const formatUsd = useCurrencyUsd();
  const list = useBudgets();
  const create = useCreateBudget();
  const [createModalOpen, setCreateModalOpen] = useState(false);

  return (
    <>
      <PageHeader
        title={t('budgets.title')}
        subtitle={t('budgets.subtitle')}
        actions={
          <Button
            type="button"
            className="success-action-button"
            onClick={() => setCreateModalOpen(true)}
          >
            <Plus /> {t('budgets.newBudget')}
          </Button>
        }
      />

      <Card>
        <CardContent>
          <h3 className="text-muted-foreground mt-0 mb-3 text-sm font-medium">
            {t('budgets.existing')}
          </h3>
          {list.isLoading ? (
            <div className="text-muted-foreground inline-flex items-center gap-2 text-sm">
              <Spinner /> {t('common.loading')}
            </div>
          ) : !list.data || list.data.items.length === 0 ? (
            <Empty>
              <EmptyHeader>
                <EmptyTitle>{t('budgets.empty')}</EmptyTitle>
                <EmptyDescription>{t('budgets.subtitle')}</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('budgets.columns.name')}</TableHead>
                  <TableHead>{t('budgets.columns.scope')}</TableHead>
                  <TableHead>{t('budgets.columns.period')}</TableHead>
                  <TableHead className="text-right">{t('budgets.columns.amount')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {list.data.items.map((b) => (
                  <TableRow key={b.id}>
                    <TableCell className="font-medium">{b.name}</TableCell>
                    <TableCell>
                      {t(`budgets.scope.${b.scopeType}`)}: {b.scopeValue}
                    </TableCell>
                    <TableCell>{t(`budgets.period.${b.period}`)}</TableCell>
                    <TableCell className="text-right">{formatUsd(b.amountUsd)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <CreateBudgetModal
        open={createModalOpen}
        onClose={() => setCreateModalOpen(false)}
        onCreate={create.mutateAsync}
        createPending={create.isPending}
        createError={create.isError ? (create.error as Error).message : null}
      />
    </>
  );
}

function CreateBudgetModal({
  open,
  onClose,
  onCreate,
  createPending,
  createError,
}: {
  open: boolean;
  onClose: () => void;
  onCreate: (input: CreateBudgetInput) => Promise<unknown>;
  createPending: boolean;
  createError: string | null;
}) {
  const { t } = useI18n();

  const [name, setName] = useState('');
  const [amountUsd, setAmountUsd] = useState(1000);
  const [scopeType, setScopeType] = useState<CreateBudgetInput['scopeType']>('provider');
  const [scopeValue, setScopeValue] = useState('*');
  const [period, setPeriod] = useState<CreateBudgetInput['period']>('monthly');
  const scopeValuePlaceholder = t(`budgets.scope.placeholder.${scopeType}`);

  const close = useCallback(() => {
    if (!createPending) onClose();
  }, [createPending, onClose]);

  const onSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    await onCreate({
      workspaceId: null,
      name,
      scopeType,
      scopeValue,
      amountUsd,
      period,
      thresholdsPct: [80, 100],
      notifyEmails: [],
    });
    onClose();
    setName('');
  };

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [close, open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/55 p-4"
      role="presentation"
      onMouseDown={close}
    >
      <form
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-budget-modal-title"
        className="bg-background border-border w-full max-w-2xl rounded-lg border shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
        onSubmit={onSubmit}
      >
        <div className="flex items-start justify-between gap-4 p-5">
          <div className="min-w-0">
            <h3 id="create-budget-modal-title" className="text-base font-semibold">
              {t('budgets.newBudget')}
            </h3>
            <p className="text-muted-foreground mt-1 mb-0 text-sm">{t('budgets.subtitle')}</p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground hover:bg-muted/40 grid size-8 place-items-center rounded-md transition-colors disabled:opacity-50"
            aria-label={t('common.close')}
            onClick={close}
            disabled={createPending}
          >
            <X className="size-4" aria-hidden="true" />
          </button>
        </div>

        <div className="p-5">
          <FieldGroup>
            <div className="grid gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="budget-name">{t('budgets.namePlaceholder')}</FieldLabel>
                <Input
                  id="budget-name"
                  required
                  placeholder={t('budgets.namePlaceholder')}
                  value={name}
                  disabled={createPending}
                  onChange={(e) => setName(e.target.value)}
                />
              </Field>

              <Field>
                <FieldLabel htmlFor="budget-amount">{t('budgets.amountPlaceholder')}</FieldLabel>
                <Input
                  id="budget-amount"
                  required
                  type="number"
                  min={1}
                  placeholder={t('budgets.amountPlaceholder')}
                  value={amountUsd}
                  disabled={createPending}
                  onChange={(e) => setAmountUsd(Number(e.target.value))}
                />
              </Field>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel>{t('budgets.columns.scope')}</FieldLabel>
                <Select
                  value={scopeType}
                  disabled={createPending}
                  onValueChange={(v) => setScopeType(v as CreateBudgetInput['scopeType'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="z-[90]">
                    {SCOPE_OPTIONS.map((option) => (
                      <SelectItem key={option} value={option}>
                        {t(`budgets.scope.${option}`)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>

              <Field>
                <FieldLabel>{t('budgets.columns.period')}</FieldLabel>
                <Select
                  value={period}
                  disabled={createPending}
                  onValueChange={(v) => setPeriod(v as CreateBudgetInput['period'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="z-[90]">
                    {PERIOD_OPTIONS.map((option) => (
                      <SelectItem key={option} value={option}>
                        {t(`budgets.period.${option}`)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
            </div>

            <Field>
              <FieldLabel htmlFor="budget-scope-value">{scopeValuePlaceholder}</FieldLabel>
              <Input
                id="budget-scope-value"
                required
                placeholder={scopeValuePlaceholder}
                value={scopeValue}
                disabled={createPending}
                onChange={(e) => setScopeValue(e.target.value)}
              />
            </Field>

            {createError ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertDescription>{createError}</AlertDescription>
              </Alert>
            ) : null}
          </FieldGroup>
        </div>

        <div className="flex justify-end gap-2 p-5 pt-0">
          <Button type="button" variant="secondary" onClick={close} disabled={createPending}>
            {t('common.cancel')}
          </Button>
          <Button type="submit" className="success-action-button" disabled={createPending}>
            {createPending ? (
              <>
                <Spinner /> {t('common.saving')}
              </>
            ) : (
              t('budgets.create')
            )}
          </Button>
        </div>
      </form>
    </div>
  );
}
