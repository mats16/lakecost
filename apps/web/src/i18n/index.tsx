import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { en, type Dictionary } from './locales/en';
import { ja } from './locales/ja';

export type Locale = 'en' | 'ja';

const DICTIONARIES: Record<Locale, Dictionary> = { en, ja };
const STORAGE_KEY = 'lakecost.locale';

export type TFunction = (key: string, params?: Record<string, string | number>) => string;

interface I18nContextValue {
  locale: Locale;
  setLocale: (next: Locale) => void;
  t: TFunction;
}

const I18nContext = createContext<I18nContextValue | null>(null);

function detectInitialLocale(): Locale {
  if (typeof window === 'undefined') return 'en';
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === 'en' || stored === 'ja') return stored;
  } catch {
    // ignore (private mode, etc.)
  }
  if (typeof navigator !== 'undefined') {
    const langs = [navigator.language, ...(navigator.languages ?? [])];
    if (langs.some((l) => l?.toLowerCase().startsWith('ja'))) return 'ja';
  }
  return 'en';
}

function lookup(dict: unknown, key: string): string | undefined {
  const parts = key.split('.');
  let cur: unknown = dict;
  for (const p of parts) {
    if (cur && typeof cur === 'object' && p in (cur as Record<string, unknown>)) {
      cur = (cur as Record<string, unknown>)[p];
    } else {
      return undefined;
    }
  }
  return typeof cur === 'string' ? cur : undefined;
}

function format(template: string, params?: Record<string, string | number>): string {
  if (!params) return template;
  return template.replace(/\{(\w+)\}/g, (_, k: string) =>
    k in params ? String(params[k]) : `{${k}}`,
  );
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(detectInitialLocale);

  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.lang = locale;
    }
  }, [locale]);

  const setLocale = useCallback((next: Locale) => {
    setLocaleState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // ignore
    }
  }, []);

  const value = useMemo<I18nContextValue>(() => {
    const dict = DICTIONARIES[locale];
    const fallback = DICTIONARIES.en;
    return {
      locale,
      setLocale,
      t: (key, params) => format(lookup(dict, key) ?? lookup(fallback, key) ?? key, params),
    };
  }, [locale, setLocale]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error('useI18n must be used within I18nProvider');
  return ctx;
}

export function useT() {
  return useI18n().t;
}

export function useLocale() {
  const { locale } = useI18n();
  return locale;
}

/** Locale-aware USD currency formatter, memoized per locale. */
const currencyFormatters = new Map<Locale, Intl.NumberFormat>();
export function formatCurrencyUsd(value: number, locale: Locale): string {
  let fmt = currencyFormatters.get(locale);
  if (!fmt) {
    fmt = new Intl.NumberFormat(locale === 'ja' ? 'ja-JP' : 'en-US', {
      style: 'currency',
      currency: 'USD',
    });
    currencyFormatters.set(locale, fmt);
  }
  return fmt.format(value);
}

export function useCurrencyUsd() {
  const locale = useLocale();
  return useCallback((value: number) => formatCurrencyUsd(value, locale), [locale]);
}
