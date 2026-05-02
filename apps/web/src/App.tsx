import { Routes, Route, Navigate } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { CostExplorer } from './pages/CostExplorer';
import { Budgets } from './pages/Budgets';
import { ConfigureLayout } from './pages/Configure/ConfigureLayout';
import { DataSources } from './pages/Configure/DataSources';
import { Catalog } from './pages/Configure/Catalog';
import { Transformations } from './pages/Configure/Transformations';

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/overview" replace />} />
        <Route path="/overview" element={<Dashboard />} />
        <Route path="/overview/budgets" element={<Budgets />} />
        <Route path="/explorer" element={<CostExplorer />} />
        <Route path="/budgets" element={<Navigate to="/overview/budgets" replace />} />

        <Route path="/configure" element={<ConfigureLayout />}>
          <Route index element={<Navigate to="data-sources" replace />} />
          <Route path="data-sources" element={<DataSources />} />
          <Route path="transformations" element={<Transformations />} />
          <Route path="catalog" element={<Catalog />} />
        </Route>

        <Route path="/settings" element={<Navigate to="/configure/catalog" replace />} />
        <Route path="/setup" element={<Navigate to="/configure/data-sources" replace />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </AppShell>
  );
}
