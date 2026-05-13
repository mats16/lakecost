import { useEffect, useState, type FormEvent } from 'react';
import {
  Alert,
  AlertDescription,
  Button,
  Card,
  CardContent,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
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
import { AlertCircle, Pencil, Plus, Trash2 } from 'lucide-react';
import { PageHeader } from '../components/PageHeader';
import { useBudgets, useCreateBudget, useDeleteBudget, useUpdateBudget } from '../api/hooks';
import {
  BudgetScopeTypeSchema,
  BudgetPeriodSchema,
  type Budget,
  type CreateBudgetInput,
  type UpdateBudgetInput,
} from '@finlake/shared';
import { useCurrencyUsd, useI18n } from '../i18n';

const SCOPE_OPTIONS = BudgetScopeTypeSchema.options;
const PERIOD_OPTIONS = BudgetPeriodSchema.options;

type BudgetFormValues = {
  name: string;
  amountUsd: number;
  scopeType: CreateBudgetInput['scopeType'];
  scopeValue: string;
  period: CreateBudgetInput['period'];
  thresholdsPct: number[];
  notifyEmails: string[];
};

export function Budgets() {
  const { t } = useI18n();
  const formatUsd = useCurrencyUsd();
  const list = useBudgets();
  const create = useCreateBudget();
  const update = useUpdateBudget();
  const deleteBudget = useDeleteBudget();
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editingBudget, setEditingBudget] = useState<Budget | null>(null);

  const openCreateModal = () => {
    create.reset();
    setCreateModalOpen(true);
  };

  const openEditModal = (budget: Budget) => {
    update.reset();
    setEditingBudget(budget);
  };

  const onDeleteBudget = (budget: Budget) => {
    if (!window.confirm(t('budgets.confirmDelete', { name: budget.name }))) return;
    deleteBudget.reset();
    deleteBudget.mutate(budget.id);
  };

  const isDeleting = (id: string) => deleteBudget.isPending && deleteBudget.variables === id;

  return (
    <>
      <PageHeader
        title={t('budgets.title')}
        subtitle={t('budgets.subtitle')}
        actions={
          <Button type="button" onClick={openCreateModal}>
            <Plus /> {t('budgets.newBudget')}
          </Button>
        }
      />

      <Card>
        <CardContent>
          <h3 className="text-muted-foreground mt-0 mb-3 text-sm font-medium">
            {t('budgets.existing')}
          </h3>
          {deleteBudget.isError ? (
            <Alert variant="destructive" className="mb-3">
              <AlertCircle />
              <AlertDescription>
                {t('budgets.deleteFailed')}: {(deleteBudget.error as Error).message}
              </AlertDescription>
            </Alert>
          ) : null}
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
                  <TableHead className="text-right" aria-label={t('budgets.columns.actions')} />
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
                    <TableCell className="text-right">
                      <div className="inline-flex items-center gap-2">
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          aria-label={t('budgets.editBudgetNamed', { name: b.name })}
                          onClick={() => openEditModal(b)}
                          disabled={isDeleting(b.id)}
                        >
                          <Pencil className="size-4" />
                          {t('budgets.edit')}
                        </Button>
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          aria-label={t('budgets.deleteBudgetNamed', { name: b.name })}
                          onClick={() => onDeleteBudget(b)}
                          disabled={isDeleting(b.id)}
                        >
                          {isDeleting(b.id) ? (
                            <Spinner className="size-4" />
                          ) : (
                            <Trash2 className="size-4" />
                          )}
                          {isDeleting(b.id) ? t('budgets.deleting') : t('budgets.delete')}
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <BudgetFormDialog
        open={createModalOpen}
        budget={null}
        title={t('budgets.newBudget')}
        submitLabel={t('budgets.create')}
        pending={create.isPending}
        error={create.isError ? (create.error as Error).message : null}
        onClose={() => setCreateModalOpen(false)}
        onSubmit={async (values) => {
          await create.mutateAsync({ workspaceId: null, ...values });
        }}
      />
      <BudgetFormDialog
        open={editingBudget !== null}
        budget={editingBudget}
        title={t('budgets.editBudget')}
        submitLabel={t('budgets.save')}
        pending={update.isPending}
        error={update.isError ? (update.error as Error).message : null}
        onClose={() => setEditingBudget(null)}
        onSubmit={async (values) => {
          if (!editingBudget) return;
          await update.mutateAsync({ id: editingBudget.id, input: values });
        }}
      />
    </>
  );
}

function BudgetFormDialog({
  open,
  budget,
  title,
  submitLabel,
  pending,
  error,
  onClose,
  onSubmit,
}: {
  open: boolean;
  budget: Budget | null;
  title: string;
  submitLabel: string;
  pending: boolean;
  error: string | null;
  onClose: () => void;
  onSubmit: (values: UpdateBudgetInput) => Promise<unknown>;
}) {
  const { t } = useI18n();

  const [name, setName] = useState('');
  const [amountUsd, setAmountUsd] = useState(1000);
  const [scopeType, setScopeType] = useState<BudgetFormValues['scopeType']>('provider');
  const [scopeValue, setScopeValue] = useState('*');
  const [period, setPeriod] = useState<BudgetFormValues['period']>('monthly');
  const scopeValuePlaceholder = t(`budgets.scope.placeholder.${scopeType}`);

  useEffect(() => {
    if (!open) return;
    setName(budget?.name ?? '');
    setAmountUsd(budget?.amountUsd ?? 1000);
    setScopeType(budget?.scopeType ?? 'provider');
    setScopeValue(budget?.scopeValue ?? '*');
    setPeriod(budget?.period ?? 'monthly');
  }, [budget, open]);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    await onSubmit({
      name,
      scopeType,
      scopeValue,
      amountUsd,
      period,
      thresholdsPct: budget?.thresholdsPct ?? [80, 100],
      notifyEmails: budget?.notifyEmails ?? [],
    });
    onClose();
  };

  const blockWhilePending = (event: Event) => {
    if (pending) event.preventDefault();
  };

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next && !pending) onClose();
      }}
    >
      <DialogContent
        className="sm:max-w-2xl"
        onEscapeKeyDown={blockWhilePending}
        onPointerDownOutside={blockWhilePending}
        onInteractOutside={blockWhilePending}
      >
        <form onSubmit={handleSubmit}>
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{t('budgets.subtitle')}</DialogDescription>
          </DialogHeader>

          <FieldGroup className="py-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="budget-name">{t('budgets.namePlaceholder')}</FieldLabel>
                <Input
                  id="budget-name"
                  required
                  placeholder={t('budgets.namePlaceholder')}
                  value={name}
                  disabled={pending}
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
                  disabled={pending}
                  onChange={(e) => setAmountUsd(Number(e.target.value))}
                />
              </Field>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel>{t('budgets.columns.scope')}</FieldLabel>
                <Select
                  value={scopeType}
                  disabled={pending}
                  onValueChange={(v) => setScopeType(v as BudgetFormValues['scopeType'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
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
                  disabled={pending}
                  onValueChange={(v) => setPeriod(v as BudgetFormValues['period'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
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
                disabled={pending}
                onChange={(e) => setScopeValue(e.target.value)}
              />
            </Field>

            {error ? (
              <Alert variant="destructive">
                <AlertCircle />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            ) : null}
          </FieldGroup>

          <DialogFooter>
            <Button type="button" variant="secondary" onClick={onClose} disabled={pending}>
              {t('common.cancel')}
            </Button>
            <Button type="submit" disabled={pending}>
              {pending ? (
                <>
                  <Spinner /> {t('common.saving')}
                </>
              ) : (
                submitLabel
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
