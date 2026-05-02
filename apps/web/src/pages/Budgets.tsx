import { useState } from 'react';
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
import {
  BudgetScopeTypeSchema,
  BudgetPeriodSchema,
  type CreateBudgetInput,
} from '@lakecost/shared';
import { useCurrencyUsd, useI18n } from '../i18n';

const SCOPE_OPTIONS = BudgetScopeTypeSchema.options;
const PERIOD_OPTIONS = BudgetPeriodSchema.options;

export function Budgets() {
  const { t } = useI18n();
  const formatUsd = useCurrencyUsd();
  const list = useBudgets();
  const create = useCreateBudget();
  const [showForm, setShowForm] = useState(false);

  const [name, setName] = useState('');
  const [amountUsd, setAmountUsd] = useState(1000);
  const [scopeType, setScopeType] = useState<CreateBudgetInput['scopeType']>('provider');
  const [scopeValue, setScopeValue] = useState('*');
  const [period, setPeriod] = useState<CreateBudgetInput['period']>('monthly');
  const scopeValuePlaceholder = t(`budgets.scope.placeholder.${scopeType}`);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await create.mutateAsync({
      workspaceId: null,
      name,
      scopeType,
      scopeValue,
      amountUsd,
      period,
      thresholdsPct: [80, 100],
      notifyEmails: [],
    });
    setShowForm(false);
    setName('');
  };

  return (
    <>
      <PageHeader title={t('budgets.title')} subtitle={t('budgets.subtitle')} />
      <Card className="mb-4">
        <CardContent>
          <Button
            type="button"
            variant={showForm ? 'outline' : 'default'}
            onClick={() => setShowForm((v) => !v)}
          >
            {showForm ? (
              <>
                <X /> {t('common.cancel')}
              </>
            ) : (
              <>
                <Plus /> {t('budgets.newBudget')}
              </>
            )}
          </Button>
          {showForm ? (
            <form onSubmit={onSubmit} className="mt-4 max-w-lg">
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="budget-name">{t('budgets.namePlaceholder')}</FieldLabel>
                  <Input
                    id="budget-name"
                    required
                    placeholder={t('budgets.namePlaceholder')}
                    value={name}
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
                    onChange={(e) => setAmountUsd(Number(e.target.value))}
                  />
                </Field>

                <Field>
                  <FieldLabel>{t('budgets.columns.scope')}</FieldLabel>
                  <Select
                    value={scopeType}
                    onValueChange={(v) => setScopeType(v as CreateBudgetInput['scopeType'])}
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
                  <FieldLabel htmlFor="budget-scope-value">{scopeValuePlaceholder}</FieldLabel>
                  <Input
                    id="budget-scope-value"
                    required
                    placeholder={scopeValuePlaceholder}
                    value={scopeValue}
                    onChange={(e) => setScopeValue(e.target.value)}
                  />
                </Field>

                <Field>
                  <FieldLabel>{t('budgets.columns.period')}</FieldLabel>
                  <Select
                    value={period}
                    onValueChange={(v) => setPeriod(v as CreateBudgetInput['period'])}
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

                <Button type="submit" disabled={create.isPending}>
                  {create.isPending ? (
                    <>
                      <Spinner /> {t('common.saving')}
                    </>
                  ) : (
                    t('budgets.create')
                  )}
                </Button>
                {create.isError ? (
                  <Alert variant="destructive">
                    <AlertCircle />
                    <AlertDescription>{(create.error as Error).message}</AlertDescription>
                  </Alert>
                ) : null}
              </FieldGroup>
            </form>
          ) : null}
        </CardContent>
      </Card>

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
    </>
  );
}
