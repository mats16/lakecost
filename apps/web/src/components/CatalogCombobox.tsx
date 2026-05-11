import { useState } from 'react';
import {
  Button,
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Spinner,
  cn,
} from '@databricks/appkit-ui/react';
import { Check, ChevronsUpDown, Plus } from 'lucide-react';
import { IDENT_RE, type CatalogSummary } from '@finlake/shared';

export type CatalogSelection = { name: string; create: boolean };

export interface CatalogComboboxProps {
  value: string;
  onChange: (selection: CatalogSelection) => void;
  options: CatalogSummary[];
  loading?: boolean;
  disabled?: boolean;
  placeholder?: string;
  searchPlaceholder?: string;
  emptyText?: string;
  createLabel?: (name: string) => string;
  /** When false the "Create …" option is hidden. Defaults to true. */
  allowCreate?: boolean;
}

export function CatalogCombobox({
  value,
  onChange,
  options,
  loading = false,
  disabled = false,
  placeholder = 'Select a catalog…',
  searchPlaceholder = 'Search catalogs…',
  emptyText = 'No catalogs found.',
  createLabel = (name) => `Create "${name}"`,
  allowCreate = true,
}: CatalogComboboxProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');

  const trimmed = query.trim();
  const exactMatch = options.some((o) => o.name === trimmed);
  const showCreate = allowCreate && trimmed.length > 0 && !exactMatch && IDENT_RE.test(trimmed);

  const handleSelect = (name: string, create: boolean) => {
    onChange({ name, create });
    setOpen(false);
    setQuery('');
  };

  const triggerLabel = value ? value : placeholder;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          type="button"
          className={cn(
            'w-full justify-between truncate font-normal',
            !value && 'text-muted-foreground',
          )}
        >
          {triggerLabel}
          {loading ? (
            <Spinner className="ml-2 size-4 shrink-0 opacity-60" />
          ) : (
            <ChevronsUpDown className="ml-2 size-4 shrink-0 opacity-60" />
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-(--radix-popover-trigger-width) p-0" align="start">
        <Command shouldFilter={true}>
          <CommandInput
            className="catalog-combobox-search"
            value={query}
            onValueChange={setQuery}
            placeholder={searchPlaceholder}
          />
          <CommandList>
            <CommandEmpty>{emptyText}</CommandEmpty>
            <CommandGroup>
              {options.map((opt) => (
                <CommandItem
                  key={opt.name}
                  value={opt.name}
                  onSelect={() => handleSelect(opt.name, false)}
                >
                  <Check
                    className={cn('mr-2 size-4', value === opt.name ? 'opacity-100' : 'opacity-0')}
                  />
                  <span className="truncate">{opt.name}</span>
                  {opt.catalogType ? (
                    <span className="text-muted-foreground ml-auto text-xs">
                      {opt.catalogType.replace(/_CATALOG$/, '').toLowerCase()}
                    </span>
                  ) : null}
                </CommandItem>
              ))}
              {showCreate ? (
                <CommandItem
                  key={`__create__${trimmed}`}
                  value={`__create__${trimmed}`}
                  onSelect={() => handleSelect(trimmed, true)}
                >
                  <Plus className="mr-2 size-4" />
                  {createLabel(trimmed)}
                </CommandItem>
              ) : null}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
