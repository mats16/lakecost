import { Routes, Route, Navigate } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { CostExplorer } from './pages/CostExplorer';
import { Budgets } from './pages/Budgets';
import { ConfigureLayout } from './pages/Configure/ConfigureLayout';
import { DataSources } from './pages/Configure/DataSources';
import { Catalog } from './pages/Configure/Catalog';
import { Transformations } from './pages/Configure/Transformations';
import { GovernedTags } from './pages/Configure/GovernedTags';
import { Credentials } from './pages/ExternalData/Credentials';

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/overview" replace />} />
        <Route path="/overview" element={<Dashboard />} />
        <Route path="/overview/budgets" element={<Budgets />} />
        <Route path="/explorer" element={<CostExplorer />} />
        <Route path="/budgets" element={<Navigate to="/overview/budgets" replace />} />

        <Route element={<ConfigureLayout />}>
          <Route path="/data-sources" element={<DataSources />} />
          <Route path="/tags" element={<GovernedTags />} />
          <Route path="/transformations" element={<Transformations />} />
          <Route path="/credentials" element={<Credentials />} />
          <Route path="/catalog" element={<Catalog />} />
        </Route>

        <Route path="/configure" element={<Navigate to="/data-sources" replace />} />
        <Route path="/configure/data-sources" element={<Navigate to="/data-sources" replace />} />
        <Route path="/configure/credentials" element={<Navigate to="/credentials" replace />} />
        <Route
          path="/configure/transformations"
          element={<Navigate to="/transformations" replace />}
        />
        <Route path="/configure/catalog" element={<Navigate to="/catalog" replace />} />

        <Route path="/storage-credentials" element={<Navigate to="/credentials" replace />} />
        <Route path="/bcm-credentials" element={<Navigate to="/credentials" replace />} />

        <Route path="/settings" element={<Navigate to="/catalog" replace />} />
        <Route path="/setup" element={<Navigate to="/data-sources" replace />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </AppShell>
  );
}
